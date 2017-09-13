/*
 * Copyright 2016 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.pubsub.clients.consumer.ack;

import com.google.api.core.AbstractApiService;
import com.google.api.core.ApiClock;
import com.google.api.core.ApiService;
import com.google.api.core.CurrentMillisClock;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.batching.FlowController.LimitExceededBehavior;
import com.google.api.gax.core.Distribution;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.threeten.bp.Duration;

/**
 * A Cloud Pub/Sub <a href="https://cloud.google.com/pubsub/docs/subscriber">subscriber</a> that is
 * associated with a specific subscription at creation.
 *
 * <p>A {@link Subscriber} allows you to provide an implementation of a {@link MessageReceiver
 * receiver} to which messages are going to be delivered as soon as they are received by the
 * subscriber. The delivered messages then can be {@link AckReplyConsumer#ack() acked} or {@link
 * AckReplyConsumer#nack() nacked} at will as they get processed by the receiver. Nacking a messages
 * implies a later redelivery of such message.
 *
 * <p>The subscriber handles the ack management, by automatically extending the ack deadline while
 * the message is being processed, to then issue the ack or nack of such message when the processing
 * is done. <strong>Note:</strong> message redelivery is still possible.
 *
 * <p>It also provides customizable options that control:
 *
 * <ul>
 *   <li>Ack deadline extension: such as the amount of time ahead to trigger the extension of
 *       message acknowledgement expiration.
 *   <li>Flow control: such as the maximum outstanding messages or maximum outstanding bytes to keep
 *       in memory before the receiver either ack or nack them.
 * </ul>
 *
 * <p>{@link Subscriber} will use the credentials set on the channel, which uses application default
 * credentials through {@link GoogleCredentials#getApplicationDefault} by default.
 */
public class Subscriber extends AbstractApiService {
  private static final int THREADS_PER_CHANNEL = 5;
  @VisibleForTesting static final int CHANNELS_PER_CORE = 1;
  private static final int MAX_INBOUND_MESSAGE_SIZE =
      20 * 1024 * 1024; // 20MB API maximum message size.
  private static final int INITIAL_ACK_DEADLINE_SECONDS = 10;
  private static final int MAX_ACK_DEADLINE_SECONDS = 600;
  static final int MIN_ACK_DEADLINE_SECONDS = 10;
  private static final Duration ACK_DEADLINE_UPDATE_PERIOD = Duration.ofMinutes(1);
  private static final double PERCENTILE_FOR_ACK_DEADLINE_UPDATES = 99.9;

  private static final ScheduledExecutorService SHARED_SYSTEM_EXECUTOR =
      InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(6).build().getExecutor();

  private static final Logger logger = Logger.getLogger(Subscriber.class.getName());

  /*private final SubscriptionName subscriptionName;
  private final String cachedSubscriptionNameString;*/
  private final Subscription subscription;
  private final FlowControlSettings flowControlSettings;
  private final Duration ackExpirationPadding;
  private final Duration maxAckExtensionPeriod;
  private final ScheduledExecutorService executor;
  @Nullable private final ScheduledExecutorService alarmsExecutor;
  private final Distribution ackLatencyDistribution =
      new Distribution(MAX_ACK_DEADLINE_SECONDS + 1);
  private final int numChannels;
  private final FlowController flowController;
  private final CallCredentials callCredentials;
  private final Channel channel;
  private final MessageReceiver receiver;
  private final PollingSubscriberConnection pollingSubscriberConnection;
  private final ApiClock clock;
  private final List<AutoCloseable> closeables = new ArrayList<>();
  private Long maxPullRecords;
  private long nextCommitTime;
  private Boolean autoCommit;
  private final Integer autoCommitIntervalMs;
  private final Long retryBackoffMs;

  private Subscriber(Builder builder) {
    receiver = builder.receiver;
    flowControlSettings = builder.flowControlSettings;
    subscription = builder.subscription;
    ackExpirationPadding = builder.ackExpirationPadding;
    maxAckExtensionPeriod = builder.maxAckExtensionPeriod;
    maxPullRecords = builder.maxPullRecords;
    clock = builder.clock.isPresent() ? builder.clock.get() : CurrentMillisClock.getDefaultClock();
    this.autoCommitIntervalMs = builder.autoCommitIntervalMs;
    this.autoCommit = builder.autoCommit;
    this.retryBackoffMs = builder.retryBackoffMs;

    if(this.autoCommit) {
      this.nextCommitTime = clock.millisTime() + autoCommitIntervalMs;
    }

    flowController =
        new FlowController(
            builder
                .flowControlSettings
                .toBuilder()
                .setLimitExceededBehavior(LimitExceededBehavior.ThrowException)
                .build());

    executor = builder.executorProvider.getExecutor();
    if (builder.executorProvider.shouldAutoClose()) {
      closeables.add(
          new AutoCloseable() {
            @Override
            public void close() throws IOException {
              executor.shutdown();
            }
          });
    }
    alarmsExecutor = builder.systemExecutorProvider.getExecutor();
    if (builder.systemExecutorProvider.shouldAutoClose()) {
      closeables.add(
          new AutoCloseable() {
            @Override
            public void close() throws IOException {
              alarmsExecutor.shutdown();
            }
          });
    }

    this.callCredentials = builder.callCredentials;
    this.channel = builder.channel;

    numChannels = builder.parallelPullCount;

    SubscriberFutureStub stub = builder.subscriberFutureStub;

    this.pollingSubscriberConnection = new PollingSubscriberConnection(
        subscription,
        receiver,
        ackExpirationPadding,
        maxAckExtensionPeriod,
        ackLatencyDistribution,
        stub,
        flowController,
        maxPullRecords,
        executor,
        alarmsExecutor,
        clock,
        retryBackoffMs);
  }

  /**
   * Constructs a new {@link Builder}.
   *
   * <p>Once {@link Builder#build} is called a gRPC stub will be created for use of the {@link
   * Subscriber}.
   *
   * @param subscription Cloud Pub/Sub subscription to bind the subscriber to
   * @param receiver an implementation of {@link MessageReceiver} used to process the received
   *     messages
   */
  public static Builder defaultBuilder(SubscriptionName subscription, MessageReceiver receiver) {
    return new Builder(subscription, receiver);
  }

  /** Subscription which the subscriber is subscribed to. */
  public SubscriptionName getSubscriptionName() {
    return subscription.getNameAsSubscriptionName();
  }

  /** Acknowledgement expiration padding. See {@link Builder#setAckExpirationPadding}. */
  public Duration getAckExpirationPadding() {
    return ackExpirationPadding;
  }

  /** The flow control settings the Subscriber is configured with. */
  public FlowControlSettings getFlowControlSettings() {
    return flowControlSettings;
  }

  /**
   * Initiates service startup and returns immediately.
   *
   * <p>Example of receiving a specific number of messages.
   *
   * <pre>{@code
   * Subscriber subscriber = Subscriber.defaultBuilder(subscription, receiver).build();
   * subscriber.addListener(new Subscriber.Listener() {
   *   public void failed(Subscriber.State from, Throwable failure) {
   *     // Handle error.
   *   }
   * }, executor);
   * subscriber.startAsync();
   *
   * // Wait for a stop signal.
   * done.get();
   * subscriber.stopAsync().awaitTerminated();
   * }</pre>
   */
  @Override
  public ApiService startAsync() {
    // Override only for the docs.
    return super.startAsync();
  }

  public void commit(boolean sync) {
    pollingSubscriberConnection.commit(sync, null);
  }


  @Override
  public void doStart() {
    logger.log(Level.FINE, "Starting subscriber group.");
    pollingSubscriberConnection.startAsync().awaitRunning();
    notifyStarted();
  }

  @Override
  public void doStop() {
    // stop connection is no-op if connections haven't been started.
    stopAllPollingConnection();
    try {
      for (AutoCloseable closeable : closeables) {
        closeable.close();
      }
      notifyStopped();
    } catch (Exception e) {
      notifyFailed(e);
    }
  }

  public PullResponse pull(final long timeout) throws IOException, ExecutionException, InterruptedException {
    long now = clock.millisTime();
    synchronized (pollingSubscriberConnection) {
      if(this.autoCommit && this.nextCommitTime <= now) {
        pollingSubscriberConnection.commit(false, null);
        this.nextCommitTime = now + this.autoCommitIntervalMs;
      }
      return pollingSubscriberConnection.pullMessages(timeout);
    }
  }

  private void stopAllPollingConnection() {
    pollingSubscriberConnection.stopAsync().awaitTerminated();
  }

  /** Builder of {@link Subscriber Subscribers}. */
  public static final class Builder {
    private static final Duration MIN_ACK_EXPIRATION_PADDING = Duration.ofMillis(100);
    private static final Duration DEFAULT_ACK_EXPIRATION_PADDING = Duration.ofMillis(500);
    private static final Duration DEFAULT_MAX_ACK_EXTENSION_PERIOD = Duration.ofMinutes(60);
    private static final long DEFAULT_MEMORY_PERCENTAGE = 20;

    static final ExecutorProvider DEFAULT_EXECUTOR_PROVIDER =
        InstantiatingExecutorProvider.newBuilder()
            .setExecutorThreadCount(
                THREADS_PER_CHANNEL
                    * CHANNELS_PER_CORE
                    * Runtime.getRuntime().availableProcessors())
            .build();

    SubscriptionName subscriptionName;
    MessageReceiver receiver;

    Duration ackExpirationPadding = DEFAULT_ACK_EXPIRATION_PADDING;
    Duration maxAckExtensionPeriod = DEFAULT_MAX_ACK_EXTENSION_PERIOD;

    FlowControlSettings flowControlSettings =
        FlowControlSettings.newBuilder()
            .setMaxOutstandingRequestBytes(
                Runtime.getRuntime().maxMemory() * DEFAULT_MEMORY_PERCENTAGE / 100L)
            .build();

    ExecutorProvider executorProvider = DEFAULT_EXECUTOR_PROVIDER;
    ExecutorProvider systemExecutorProvider = FixedExecutorProvider.create(SHARED_SYSTEM_EXECUTOR);
    Optional<ApiClock> clock = Optional.absent();
    int parallelPullCount = Runtime.getRuntime().availableProcessors() * CHANNELS_PER_CORE;
    private Subscription subscription;
    private Long maxPullRecords;
    private Boolean autoCommit = true;
    private Integer autoCommitIntervalMs = 5000;
    private SubscriberFutureStub subscriberFutureStub;
    private Channel channel;
    private CallCredentials callCredentials;
    private Long retryBackoffMs;

    Builder(SubscriptionName subscriptionName, MessageReceiver receiver) {
      this.subscriptionName = subscriptionName;
      this.receiver = receiver;
    }

    /** Sets the flow control settings. */
    public Builder setFlowControlSettings(FlowControlSettings flowControlSettings) {
      this.flowControlSettings = Preconditions.checkNotNull(flowControlSettings);
      return this;
    }

    /**
     * Set acknowledgement expiration padding.
     *
     * <p>This is the time accounted before a message expiration is to happen, so the {@link
     * Subscriber} is able to send an ack extension beforehand.
     *
     * <p>This padding duration is configurable so you can account for network latency. A reasonable
     * number must be provided so messages don't expire because of network latency between when the
     * ack extension is required and when it reaches the Pub/Sub service.
     *
     * @param ackExpirationPadding must be greater or equal to {@link #MIN_ACK_EXPIRATION_PADDING}
     */
    public Builder setAckExpirationPadding(Duration ackExpirationPadding) {
      Preconditions.checkArgument(ackExpirationPadding.compareTo(MIN_ACK_EXPIRATION_PADDING) >= 0);
      this.ackExpirationPadding = ackExpirationPadding;
      return this;
    }

    /**
     * Set the maximum period a message ack deadline will be extended.
     *
     * <p>It is recommended to set this value to a reasonable upper bound of the subscriber time to
     * process any message. This maximum period avoids messages to be <i>locked</i> by a subscriber
     * in cases when the ack reply is lost.
     *
     * <p>A zero duration effectively disables auto deadline extensions.
     */
    public Builder setMaxAckExtensionPeriod(Duration maxAckExtensionPeriod) {
      Preconditions.checkArgument(maxAckExtensionPeriod.toMillis() >= 0);
      this.maxAckExtensionPeriod = maxAckExtensionPeriod;
      return this;
    }

    /**
     * Gives the ability to set a custom executor for polling and managing lease extensions. If none
     * is provided a shared one will be used by all {@link Subscriber} instances.
     */
    public Builder setSystemExecutorProvider(ExecutorProvider executorProvider) {
      this.systemExecutorProvider = Preconditions.checkNotNull(executorProvider);
      return this;
    }

    public Builder setSubscription(Subscription subscription) {
      this.subscription = subscription;
      return this;
    }
    
    public Builder setAutoCommit(Boolean autoCommit) {
      this.autoCommit = autoCommit;
      return this;
    }

    /** Gives the ability to set a custom clock. */
    Builder setClock(ApiClock clock) {
      this.clock = Optional.of(clock);
      return this;
    }

    public Builder setMaxPullRecords(Long maxPullRecords) {
      this.maxPullRecords = maxPullRecords;
      return this;
    }

    public Builder setAutoCommitIntervalMs(Integer autoCommitIterval) {
      this.autoCommitIntervalMs = autoCommitIterval;
      return this;
    }

    public Builder setSubscriberFutureStub(SubscriberFutureStub subscriberFutureStub) {
      this.subscriberFutureStub = subscriberFutureStub;
      return this;
    }

    public Builder setChannel(Channel channel) {
      this.channel = channel;
      return this;
    }

    public Builder setCallCredentials(CallCredentials callCredentials) {
      this.callCredentials = callCredentials;
      return this;
    }
    
    public Builder setRetryBackoffMs(Long retryBackoffMs) {
      this.retryBackoffMs = retryBackoffMs;
      return this;
    }

    public Subscriber build() {
      return new Subscriber(this);
    }
  }
}

