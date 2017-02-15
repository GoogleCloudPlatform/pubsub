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

package com.google.pubsub.clients.experimental;

import com.beust.jcommander.JCommander;
import com.google.cloud.pubsub.Publisher;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Durations;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.clients.common.Task.RunResult;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.v1.PubsubMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs a task that publishes messages to a Cloud Pub/Sub topic. */
class CPSPublisherTask extends Task {
  private static final Logger log = LoggerFactory.getLogger(CPSPublisherTask.class);
  private Publisher publisher;
  private final int batchSize;
  private final ByteString payload;
  private final Integer id;
  private final AtomicInteger sequenceNumber = new AtomicInteger(0);

  private CPSPublisherTask(StartRequest request) {
    super(request, "experimental", MetricsHandler.MetricName.PUBLISH_ACK_LATENCY);
    this.batchSize = request.getPublishBatchSize();
    this.id = (new Random()).nextInt();
    try {
      this.publisher =
          Publisher.Builder.newBuilder(
                  "projects/" + request.getProject() + "/topics/" + request.getTopic())
              .setMaxBatchDuration(
                  Duration.millis(Durations.toMillis(request.getPublishBatchDuration())))
              .setMaxBatchBytes(9500000)
              .setMaxBatchMessages(950)
              .setMaxOutstandingBytes(1000000000) // 1 GB
              .build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    this.payload = ByteString.copyFromUtf8(LoadTestRunner.createMessage(request.getMessageSize()));
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, CPSPublisherTask::new);
  }

  @Override
  public ListenableFuture<RunResult> doRun() {
    try {
      List<ListenableFuture<String>> results = new ArrayList<>();
      String sendTime = String.valueOf(System.currentTimeMillis());
      for (int i = 0; i < batchSize; i++) {
        results.add(
            publisher.publish(
                PubsubMessage.newBuilder()
                    .setData(payload)
                    .putAttributes("sendTime", sendTime)
                    .putAttributes("clientId", id.toString())
                    .putAttributes(
                        "sequenceNumber", Integer.toString(sequenceNumber.getAndIncrement()))
                    .build()));
      }
      return Futures.transform(
          Futures.allAsList(results), response -> RunResult.fromBatchSize(batchSize));
    } catch (Throwable t) {
      log.error("Flow control error.", t);
      return Futures.immediateFailedFuture(t);
    }
  }

  @Override
  protected void shutdown() {
    publisher.shutdown();
  }
}
