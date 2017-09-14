// Copyright 2017 Google Inc.
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

package com.google.pubsub.clients.mapped;

import com.beust.jcommander.JCommander;

import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ListenableFuture;

import com.google.protobuf.util.Durations;

import com.google.pubsub.clients.common.Task;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler.MetricName;

import com.google.pubsub.clients.producer.KafkaProducer;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a task that publishes messages utilizing Pub/Sub's implementation of the Kafka Producer<K,V> Interface.
 */
class CPSPublisherTask extends Task {

  private static final Logger log = LoggerFactory.getLogger(CPSPublisherTask.class);

  private final int batchSize;
  private final int messageSize;
  private final long batchDelay;

  private final String topic;
  private final String payload;
  private final String clientId;
  private final AtomicInteger sequenceNumber = new AtomicInteger(0);

  private final KafkaProducer<String, String> publisher;

  @SuppressWarnings("unchecked")
  private CPSPublisherTask(StartRequest request) {
    super(request, "mapped", MetricName.PUBLISH_ACK_LATENCY);

    this.topic = request.getTopic();
    this.messageSize = request.getMessageSize();
    this.batchSize = request.getPublishBatchSize();
    this.payload = LoadTestRunner.createMessage(messageSize);
    this.clientId = Integer.toString((new Random()).nextInt());
    this.batchDelay = Durations.toMillis(request.getPublishBatchDuration());

    Properties props = new Properties();
    props.put("project", "dataproc-kafka-test");
    props.put("linger.ms", Long.toString(batchDelay));
    props.put("elements.count", Integer.toString(batchSize));
    props.put("batch.size", Integer.toString(9500000));
    props.put("client.id", clientId);
    props.put("buffer.memory", Integer.toString(1000 * 1000 * 1000));
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    this.publisher = new KafkaProducer<>(props);
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, CPSPublisherTask::new);
  }

  @Override
  public ListenableFuture<RunResult> doRun() {

    String sendTime = String.valueOf(System.currentTimeMillis());
    AtomicInteger messagesSent = new AtomicInteger(batchSize);
    AtomicInteger messagesToSend = new AtomicInteger(batchSize);
    final SettableFuture<RunResult> done = SettableFuture.create();

    for (int i = 0; i < batchSize; i++) {
      publisher.send(
          new ProducerRecord<>(topic, 0, clientId + "#" +
              sendTime + "#" + Integer.toString(sequenceNumber.getAndIncrement()), payload),
          (recordMetadata, e) -> {
            if (e != null) {
              messagesSent.decrementAndGet();
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
