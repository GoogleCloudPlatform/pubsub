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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.util.Durations;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.clients.common.Task.RunResult;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

  private final int batchSize;

  private final String topic;
  private final String payload;
  private final String clientId;
  private final AtomicInteger sequenceNumber = new AtomicInteger(0);

  private final int partitions;
  private final KafkaProducer<String, String> publisher;

  private KafkaPublisherTask(StartRequest request) {
    super(request, "kafka", MetricsHandler.MetricName.PUBLISH_ACK_LATENCY);
    this.topic = request.getTopic();
    this.batchSize = request.getPublishBatchSize();
    this.clientId = Integer.toString((new Random()).nextInt());
    this.partitions = request.getKafkaOptions().getPartitions();
    this.payload = LoadTestRunner.createMessage(request.getMessageSize());
    Properties props = new Properties();
    props.putAll(new ImmutableMap.Builder<>()
        .put("acks", "all")
        .put("max.block.ms", "30000")
        .put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("bootstrap.servers", request.getKafkaOptions().getBroker())
        .put("buffer.memory", Integer.toString(1000 * 1000 * 1000)) // 1 GB
        .put("linger.ms", Long.toString(Durations.toMillis(request.getPublishBatchDuration())))
        .put("batch.size", Integer.toString(request.getMessageSize() * request.getPublishBatchSize()))
        .build()
    );
    this.publisher = new KafkaProducer<>(props);
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, KafkaPublisherTask::new);
  }

  @Override
  public ListenableFuture<RunResult> doRun() {

    AtomicInteger messagesSent = new AtomicInteger(batchSize);
    AtomicInteger messagesToSend = new AtomicInteger(batchSize);
    String sendTime = String.valueOf(System.currentTimeMillis());
    final SettableFuture<RunResult> done = SettableFuture.create();

    for (int i = 0; i < batchSize; i++) {
      actualCounter.incrementAndGet();
      publisher.send(
          new ProducerRecord<>(topic, sequenceNumber.get() % partitions, clientId + "#" +
              Integer.toString(sequenceNumber.getAndIncrement()) + "#" + sendTime, payload),
          (recordMetadata, e) -> {
            if (e != null) {
              messagesSent.getAndDecrement();
              log.error(e.getMessage(), e);
            }
            if (messagesToSend.decrementAndGet() == 0) {
              done.set(RunResult.fromBatchSize(messagesSent.get()));
            }
          });
    }
    return done;
  }

  @Override
  public void shutdown() {
    publisher.close();
  }
}
