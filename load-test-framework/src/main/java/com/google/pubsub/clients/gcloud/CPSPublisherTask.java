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

import com.google.cloud.pubsub.Message;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubException;
import com.google.cloud.pubsub.PubSubOptions;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Runs a task that publishes messages to a Cloud Pub/Sub topic.
 */
class CPSPublisherTask implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(CPSPublisherTask.class);
  private final String topic;
  private final MetricsHandler metricsHandler;
  private final PubSub pubSub;
  private final String payload;
  private final int batchSize;

  private CPSPublisherTask(String project, String topic, int messageSize, int batchSize) {
    this.pubSub = PubSubOptions.builder()
        .projectId(project)
        .build().service();
    this.metricsHandler = new MetricsHandler(Preconditions.checkNotNull(project), "gcloud");
    this.topic = Preconditions.checkNotNull(topic);
    this.payload = LoadTestRunner.createMessage(messageSize);
    this.batchSize = batchSize;
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.run(request ->
        new CPSPublisherTask(request.getProject(), request.getTopic(),
            request.getMessageSize(), request.getMaxMessagesPerPull())
    );
  }

  @Override
  public void run() {
    try {
      List<Message> messages = new ArrayList<>(batchSize);
      String sendTime = String.valueOf(System.currentTimeMillis());
      for (int i = 0; i < batchSize; i++) {
        messages.add(Message.builder(payload).addAttribute("sendTime", sendTime).build());
      }
      Stopwatch stopwatch = Stopwatch.createStarted();
      pubSub.publish(topic, messages);
      stopwatch.stop();
      metricsHandler.recordPublishLatency(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    } catch (PubSubException e) {
      log.error("Publish request failed", e);
    }
  }
}
