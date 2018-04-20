// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////
package com.google.pubsub.clients.gcloud;

import com.beust.jcommander.JCommander;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.Subscriber.Builder;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.LoadTestSubscriptionAdminSettings;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/** Runs a task that consumes messages from a Cloud Pub/Sub subscription. */
class CPSSubscriberTask extends Task implements MessageReceiver {

  private static final Logger log = LoggerFactory.getLogger(CPSSubscriberTask.class);
  private final SubscriptionName subscription;
  private Subscriber subscriber;
  private boolean shuttingDown = false;
  private final String emulatorHost;

  private static final int MAX_INBOUND_MESSAGE_SIZE =
      20 * 1024 * 1024; // 20MB API maximum message size.

  private CPSSubscriberTask(StartRequest request) {
    super(request, "gcloud", MetricsHandler.MetricName.END_TO_END_LATENCY);
    this.subscription = SubscriptionName.of(request.getProject(),
        request.getPubsubOptions().getSubscription());
    this.emulatorHost = request.getEmulatorHost();
    try {
      this.subscriber = getSubscriber();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void receiveMessage(final PubsubMessage message, final AckReplyConsumer consumer) {
    recordMessageLatency(
        Integer.parseInt(message.getAttributesMap().get("clientId")),
        Integer.parseInt(message.getAttributesMap().get("sequenceNumber")),
        System.currentTimeMillis() - Long.parseLong(message.getAttributesMap().get("sendTime")));
    consumer.ack();
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, CPSSubscriberTask::new);
  }

  private Subscriber getSubscriber() {
    Builder builder = Subscriber.newBuilder(subscription, this)
        .setParallelPullCount(Runtime.getRuntime().availableProcessors() * 5);

    if (Objects.nonNull(this.emulatorHost)) {
      builder.setChannelProvider(
          LoadTestSubscriptionAdminSettings.defaultGrpcTransportBuilder()
              .setEndpoint(this.emulatorHost)
              .setMaxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE)
              .setKeepAliveTime(Duration.ofMinutes(5))
              .build());
      builder.setCredentialsProvider(new NoCredentialsProvider());
    }

    return builder.build();
  }

  @Override
  public ListenableFuture<RunResult> doRun() {
    synchronized (this) {
      if (subscriber.isRunning()) {
        return Futures.immediateFuture(RunResult.empty());
      }
      if (shuttingDown) {
        return Futures.immediateFailedFuture(
            new IllegalStateException("the task is shutting down"));
      }
      try {
        subscriber.startAsync().awaitRunning();
      } catch (Exception e) {
        log.error("Fatal error from subscriber.", e);
        subscriber = Subscriber.defaultBuilder(this.subscription, this).build();
        return Futures.immediateFailedFuture(e);
      }
      return Futures.immediateFuture(RunResult.empty());
    }
  }

  @Override
  public void shutdown() {
    Subscriber subscriber;
    synchronized (this) {
      if (shuttingDown) {
        throw new IllegalStateException("the task is already shutting down");
      }
      shuttingDown = true;
      subscriber = this.subscriber;
    }
    // We must stop out of the lock. Stopping waits for all messages to be processed,
    // and processing the messages needs to lock.
    subscriber.stopAsync().awaitTerminated();
  }
}
