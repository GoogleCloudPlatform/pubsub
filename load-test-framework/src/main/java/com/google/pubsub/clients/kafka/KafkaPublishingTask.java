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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.flic.common.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Runs a task that publishes messages utilizing Kafka's implementation of the Producer<K,V>
 * interface
 */
public class KafkaPublishingTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(KafkaPublishingTask.class.getName());
  private static final String PRODUCER_PROPERTIES = "/producer.properties";
  private final String topic;
  private final MetricsHandler metricsHandler;
  private final String payload;
  private KafkaProducer<String, String> publisher;

  private KafkaPublishingTask(String broker, String project, String topic, int messageSize) {
    this.metricsHandler = new MetricsHandler(project);
    this.topic = topic;
    this.payload = Utils.createMessage(messageSize);
    Properties props = new Properties();
    InputStream is = Utils.class.getResourceAsStream(PRODUCER_PROPERTIES);
    try {
      props.load(is);
    } catch (IOException e) {
      log.warn("Unable to load producer properties file, using sane defaults.");
      props.putAll(ImmutableMap.of(
          "max.block.ms", "30000",
          "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
          "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
          "acks", "all"
      ));
    }
    props.put("bootstrap.servers", broker);
    this.publisher = new KafkaProducer<>(props);
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.run(request ->
        new KafkaPublishingTask(request.getSubscription(), request.getProject(),
            request.getTopic(), request.getMessageSize()));
  }

  @Override
  public void run() {
    Stopwatch stopwatch = Stopwatch.createStarted();
    publisher.send(
        new ProducerRecord<>(
            topic,
            null,
            System.currentTimeMillis(),
            null,
            payload),
        (metadata, exception) -> {
          stopwatch.stop();
          if (exception != null) {
            log.error(exception.getMessage(), exception);
            return;
          }
          metricsHandler.recordPublishLatency(stopwatch.elapsed(TimeUnit.MILLISECONDS));
        });
  }
}
