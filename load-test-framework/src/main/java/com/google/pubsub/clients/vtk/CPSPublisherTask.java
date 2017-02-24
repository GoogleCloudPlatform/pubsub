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

package com.google.pubsub.clients.vtk;

import com.beust.jcommander.JCommander;
import com.google.api.gax.core.RpcFutureCallback;
import com.google.cloud.pubsub.spi.v1.PublisherClient;
import com.google.cloud.pubsub.spi.v1.PublisherSettings;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Durations;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
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
  private final String topic;
  private final PublisherClient publisherApi;
  private final ByteString payload;
  private final int batchSize;
  private final Integer id;
  private final AtomicInteger sequenceNumber = new AtomicInteger(0);

  private CPSPublisherTask(StartRequest request) {
    super(request, "vtk", MetricsHandler.MetricName.PUBLISH_ACK_LATENCY);
    PublisherSettings.Builder publisherSettingsBuilder = PublisherSettings.defaultBuilder();
    publisherSettingsBuilder
        .publishSettings()
        .getBundlingSettingsBuilder()
        .setDelayThreshold(Duration.millis(Durations.toMillis(request.getPublishBatchDuration())))
        .setElementCountThreshold(950L)
        .setRequestByteThreshold(9500000L);
    try {
      this.publisherApi = PublisherClient.create(publisherSettingsBuilder.build());
    } catch (Exception e) {
      throw new RuntimeException("Error creating publisher API.", e);
    }
    this.topic = TopicName.create(request.getProject(), request.getTopic()).toString();
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
    List<PubsubMessage> messages = new ArrayList<>(batchSize);
    String sendTime = String.valueOf(System.currentTimeMillis());
    for (int i = 0; i < batchSize; i++) {
      messages.add(
          PubsubMessage.newBuilder()
              .setData(payload)
              .putAttributes("sendTime", sendTime)
              .putAttributes("clientId", id.toString())
              .putAttributes("sequenceNumber", Integer.toString(sequenceNumber.getAndIncrement()))
              .build());
    }
    PublishRequest request =
        PublishRequest.newBuilder().setTopic(topic).addAllMessages(messages).build();
    SettableFuture<RunResult> result = SettableFuture.create();
    publisherApi
        .publishCallable()
        .futureCall(request)
        .addCallback(
            new RpcFutureCallback<PublishResponse>() {
              @Override
              public void onSuccess(PublishResponse s) {
                result.set(RunResult.fromBatchSize(batchSize));
              }

              @Override
              public void onFailure(Throwable t) {
                result.setException(t);
              }
            });
    return result;
  }
}
