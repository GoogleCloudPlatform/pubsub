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

import com.google.common.collect.ImmutableMap;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.flic.common.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Runs a task that consumes messages utilizing Kafka's implementation of the Consumer<K,V>
 * interface.
 */
public class KafkaConsumerTask implements Runnable {
  private static final String CONSUMER_PROPERTIES = "/consumer.properties";
  private final Logger log = LoggerFactory.getLogger(KafkaConsumerTask.class);
  private final long pollLength;

  private final KafkaConsumer<String, String> subscriber;
  private final MetricsHandler metricsHandler;

  private KafkaConsumerTask(String broker, String project, String topic, long pollLength) {
    this.metricsHandler = new MetricsHandler(project);
    this.pollLength = pollLength;

    // Create subscriber
    Properties props = new Properties();
    try {
      props.load(Utils.class.getResourceAsStream(CONSUMER_PROPERTIES));
    } catch (IOException e) {
      log.warn("Unable to read consumer properties file, using sane defaults.");
      props.putAll(ImmutableMap.of(
          "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
          "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
          "group.id", "SUBSCRIBER_ID",
          "enable.auto.commit", "true",
          "session.timeout.ms", "30000"
      ));
    }
    props.put("bootstrap.servers", broker);
    subscriber = new KafkaConsumer<>(props);
    subscriber.subscribe(Collections.singletonList(topic));
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.run(request ->
        new KafkaConsumerTask(request.getSubscription(), request.getProject(),
            request.getTopic(), request.getRequestRate()));
  }

  @Override
  public void run() {
    ConsumerRecords<String, String> records = subscriber.poll(pollLength);
    List<Long> endToEndLatencies = new ArrayList<>();
    long now = System.currentTimeMillis();
    records.forEach((record) -> {
      endToEndLatencies.add(now - record.timestamp());
    });
    endToEndLatencies.stream().distinct().forEach(metricsHandler::recordEndToEndLatency);
    subscriber.commitAsync();
  }
}
