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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.util.Durations;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Runs a task that consumes messages utilizing Kafka's implementation of the Consumer<K,V>
 * interface.
 */
class KafkaSubscriberTask extends Task {

  private final long pollLength;
  private final KafkaConsumer<String, String> subscriber;

  private KafkaSubscriberTask(StartRequest request) {
    super(request, "kafka", MetricsHandler.MetricName.END_TO_END_LATENCY);

    this.pollLength = Durations.toMillis(request.getKafkaOptions().getPollDuration());

    Properties props = new Properties();
    props.putAll(ImmutableMap.of(
        "group.id", "SUBSCRIBER_ID",
        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
        "enable.auto.commit", "true",
        "session.timeout.ms", "30000"
    ));
    props.put("bootstrap.servers", request.getKafkaOptions().getBroker());
    subscriber = new KafkaConsumer<>(props);
    subscriber.subscribe(Collections.singletonList(request.getTopic()));
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, KafkaSubscriberTask::new);
  }

  @Override
  public ListenableFuture<RunResult> doRun() {
    ConsumerRecords<String, String> records = subscriber.poll(pollLength);
    records.forEach(
        record -> {
          String[] tokens = record.key().split("#");
          long receiveTime = System.currentTimeMillis();
          recordMessageLatency(
              Integer.parseInt(tokens[0]),
              Integer.parseInt(tokens[1]),
              record.timestamp(),
              receiveTime,
              receiveTime - Long.parseLong(tokens[2]));
        });
    return Futures.immediateFuture(RunResult.empty());
  }

  @Override
  public void shutdown() {
    subscriber.close();
  }
}
