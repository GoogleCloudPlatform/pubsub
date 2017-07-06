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
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Durations;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.clients.common.Task.RunResult;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * Runs a task that publishes messages to a Cloud Pub/Sub topic.
 */
class CPSPublisherTask extends Task {
  private static final Logger log = LoggerFactory.getLogger(CPSPublisherTask.class);
  private final Publisher publisher;
  private final ByteString payload;
  private final int batchSize;
  private final Integer id;
  private final AtomicInteger sequenceNumber = new AtomicInteger(0);

  private CPSPublisherTask(StartRequest request) {
    super(request, "gcloud", MetricsHandler.MetricName.PUBLISH_ACK_LATENCY);
    try {
      this.publisher =
          Publisher.defaultBuilder(TopicName.create(request.getProject(), request.getTopic()))
              .setBatchingSettings(
                  BatchingSettings.newBuilder()
                      .setDelayThreshold(
                          Duration.ofMillis(Durations.toMillis(request.getPublishBatchDuration())))
                      .setRequestByteThreshold(9500000L)
                      .setElementCountThreshold(950L)
                      .build())
              .build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    this.payload = ByteString.copyFromUtf8(LoadTestRunner.createMessage(request.getMessageSize()));
    this.batchSize = request.getPublishBatchSize();
    this.id = (new Random()).nextInt();
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, CPSPublisherTask::new);
  }

  @Override
  public ListenableFuture<RunResult> doRun() {
    try {
      AtomicInteger numPending = new AtomicInteger(batchSize);
      final SettableFuture<RunResult> done = SettableFuture.create();
      String sendTime = String.valueOf(System.currentTimeMillis());
      for (int i = 0; i < batchSize; i++) {
        ApiFutures.addCallback(
            publisher.publish(
                PubsubMessage.newBuilder()
                    .setData(payload)
                    .putAttributes("sendTime", sendTime)
                    .putAttributes("clientId", id.toString())
                    .putAttributes(
                        "sequenceNumber", Integer.toString(sequenceNumber.getAndIncrement()))
                    .build()),
            new ApiFutureCallback<String>() {
              @Override
              public void onSuccess(String s) {
                if (numPending.decrementAndGet() == 0) {
                  done.set(RunResult.fromBatchSize(batchSize));
                }
              }

              @Override
              public void onFailure(Throwable t) {
                done.setException(t);
              }
            });
      }
      return done;
    } catch (Throwable t) {
      log.error("Flow control error.", t);
      return Futures.immediateFailedFuture(t);
    }
  }

  @Override
  protected void shutdown() {
    try {
      publisher.shutdown();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
