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
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import org.apache.kafka.clients.producer.Callback;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a task that publishes messages utilizing Kafka's implementation of the Producer<K,V>
 * interface
 */
class KafkaPublisherTask extends Task {

  private static final Logger log = LoggerFactory.getLogger(KafkaPublisherTask.class);
  private final String topic;
  private final String payload;
  private final int batchSize;
  private final KafkaProducer<String, String> publisher;

  private KafkaPublisherTask(String broker, String project, String topic, int messageSize, 
      int batchSize) {
    super(project, "kafka", MetricsHandler.MetricName.PUBLISH_ACK_LATENCY);
    this.topic = topic;
    this.payload = LoadTestRunner.createMessage(messageSize);
    this.batchSize = batchSize;
    Properties props = new Properties();
    props.putAll(new ImmutableMap.Builder<>().
        put("max.block.ms", "30000").
        put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer").
        put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer").
        put("acks", "all").
        put("bootstrap.servers", broker).
        put("batch.size", Integer.toString(batchSize)).build());
    this.publisher = new KafkaProducer<>(props);
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, request ->
        new KafkaPublisherTask(request.getKafkaOptions().getBroker(), request.getProject(),
            request.getTopic(), request.getMessageSize(), request.getPublishBatchSize()));
  }

  @Override
  public void run() {
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    Callback callback = (metadata, exception) -> {
      if (exception != null) {
        log.error(exception.getMessage(), exception);
        return;
      }
      addNumberOfMessages(1);
      metricsHandler.recordLatency(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    };
    stopwatch.start();
    for (int i = 0; i < batchSize; i++) {
      publisher.send(
          new ProducerRecord<>(topic, null, System.currentTimeMillis(), null, payload), callback);
    }
    publisher.flush();
    stopwatch.stop();
  }
}
