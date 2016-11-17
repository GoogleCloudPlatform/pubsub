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

package com.google.cloud.pubsub;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Channel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.net.ssl.SSLException;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of {@link Subscriber}. */
public class SubscriberImpl extends AbstractService implements Subscriber {
  private static final int DEFAULT_MIN_THREAD_POOL_SIZE = 5;

  private static final Logger logger = LoggerFactory.getLogger(SubscriberImpl.class);

  private final String subscription;
  private final Optional<Integer> maxOutstandingBytes;
  private final Optional<Integer> maxOutstandingMessages;
  private final Duration ackExpirationPadding;
  private final SubscriberConnection[] subscribers;
  private final ScheduledExecutorService executor;
  private static final int MAX_INBOUND_MESSAGE_SIZE =
      20 * 1024 * 1024; // 20MB API maximum message size.

  public SubscriberImpl(SubscriberImpl.Builder builder) {
    maxOutstandingBytes = builder.maxOutstandingBytes;
    maxOutstandingMessages = builder.maxOutstandingMessages;
    subscription = builder.subscription;
    ackExpirationPadding = builder.ackExpirationPadding;

    FlowController flowController =
        new FlowController(builder.maxOutstandingBytes, builder.maxOutstandingBytes, false);

    int numCores = Math.max(1, Runtime.getRuntime().availableProcessors());
    executor =
        builder.executor.isPresent()
            ? builder.executor.get()
            : Executors.newScheduledThreadPool(
                (numCores * DEFAULT_MIN_THREAD_POOL_SIZE) + 1,
                new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("cloud-pubsub-subscriber-thread-%d")
                    .build());
    subscribers = new SubscriberConnection[numCores];

    Channel channel;
    try {
      channel =
          builder.channel.isPresent()
              ? builder.channel.get()
              : NettyChannelBuilder.forAddress(PUBSUB_API_ADDRESS, 443)
                  .maxMessageSize(MAX_INBOUND_MESSAGE_SIZE)
                  .flowControlWindow(5000000) // 2.5 MB
                  .negotiationType(NegotiationType.TLS)
                  .sslContext(GrpcSslContexts.forClient().ciphers(null).build())
                  .executor(executor)
                  .build();
    } catch (SSLException e) {
      throw new RuntimeException("Failed to initialize gRPC channel.", e);
    }

    Credentials credentials;
    try {
      credentials =
          builder.credentials.isPresent()
              ? builder.credentials.get()
              : GoogleCredentials.getApplicationDefault()
                  .createScoped(Collections.singletonList(PUBSUB_API_SCOPE));
    } catch (IOException e) {
      throw new RuntimeException("Failed to get application default credentials.", e);
    }
    for (int i = 0; i < subscribers.length; i++) {
      subscribers[i] =
          new SubscriberConnection(
              subscription,
              credentials,
              builder.receiver,
              ackExpirationPadding,
              channel,
              flowController,
              executor);
    }
  }

  @Override
  protected void doStart() {
    logger.debug("Starting subscriber group.");

    CountDownLatch subscribersStarting = new CountDownLatch(subscribers.length);
    for (SubscriberConnection subscriber : subscribers) {
      executor.submit(
          new Runnable() {
            @Override
            public void run() {
              subscriber.startAsync().awaitRunning();
              subscribersStarting.countDown();
            }
          });
    }
    try {
      subscribersStarting.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    notifyStarted();
  }

  @Override
  protected void doStop() {
    CountDownLatch subscribersStopping = new CountDownLatch(subscribers.length);
    for (SubscriberConnection subscriber : subscribers) {
      executor.submit(
          new Runnable() {
            @Override
            public void run() {
              subscriber.stopAsync().awaitTerminated();
              subscribersStopping.countDown();
            }
          });
    }
    try {
    subscribersStopping.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    notifyStopped();
  }

  @Override
  public SubscriberStats getStats() {
    // TODO: Implement me
    return null;
  }

  @Override
  public String getSubscription() {
    return subscription;
  }

  @Override
  public Duration getAckExpirationPadding() {
    return ackExpirationPadding;
  }

  @Override
  public Optional<Integer> getMaxOutstandingMessages() {
    return maxOutstandingMessages;
  }

  @Override
  public Optional<Integer> getMaxOutstandingBytes() {
    return maxOutstandingBytes;
  }
}
