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
package com.google.pubsub.clients.kafka;

import com.beust.jcommander.JCommander;
import com.google.common.collect.ImmutableMap;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Runs a task that consumes messages utilizing Kafka's implementation of the Consumer<K,V>
 * interface.
 */
class KafkaSubscriberTask extends Task {
  private final long pollLength;
  private final ConcurrentLinkedQueue<KafkaConsumer<String, String>> queue;

  private KafkaSubscriberTask(String broker, String project, String topic, String consumerGroup,
      long pollLength, int consumerCount, int pullSize, int maxFetchMs) {
    super(project, "kafka", MetricsHandler.MetricName.END_TO_END_LATENCY);
    this.pollLength = pollLength;

    // Create subscriber
    Properties props = new Properties();
    props.putAll(new ImmutableMap.Builder<>()
        .put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        .put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        .put("group.id", consumerGroup)
        .put("max.partition.fetch.bytes", pullSize)
        .put("fetch.wait.max.ms", maxFetchMs)
        .put("enable.auto.commit", "true")
        .put("session.timeout.ms", "30000")
        .put("auto.offset.reset", "latest")
        .put("bootstrap.servers", broker).build());
    queue = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < consumerCount; i++) {
      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
      consumer.subscribe(Collections.singletonList(topic));
      queue.add(consumer);
    }
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, request ->
        new KafkaSubscriberTask(request.getKafkaOptions().getBroker(), request.getProject(),
            request.getTopic(), request.getSubscription(),
            request.getKafkaOptions().getPollLength(), request.getMaxOutstandingRequests(),
            request.getMessageSize() * request.getMaxMessagesPerPull(),
            request.getKafkaOptions().getMaxFetchMs()));
  }

  @Override
  public void run() {
    KafkaConsumer<String, String> consumer = queue.poll();
    while (consumer == null) { // Shouldn't ever happen, included just in case
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) { }
      consumer = queue.poll();
    }
    ConsumerRecords<String, String> records = consumer.poll(pollLength);
    addNumberOfMessages(records.count());
    records.forEach(record ->
        metricsHandler.recordLatency(System.currentTimeMillis() - record.timestamp()));
    queue.add(consumer);
  }

  private int getConsumerIndex() {
    return 0;
  }
}
