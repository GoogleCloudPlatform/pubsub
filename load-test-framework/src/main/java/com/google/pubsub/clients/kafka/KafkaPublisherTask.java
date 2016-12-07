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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Callback;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
  private final ExecutorService callbackPool;

  private KafkaPublisherTask(StartRequest request) {
    super(request, "kafka", MetricsHandler.MetricName.PUBLISH_ACK_LATENCY);
    this.topic = request.getTopic();
    this.payload = LoadTestRunner.createMessage(request.getMessageSize());
    this.batchSize = request.getPublishBatchSize();
    Properties props = new Properties();
    props.putAll(new ImmutableMap.Builder<>()
        .put("max.block.ms", "30000")
        .put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("acks", "all")
        .put("bootstrap.servers", request.getKafkaOptions().getBroker())
        .put("batch.size", batchSize * request.getMessageSize()) // Kafka takes batch size in bytes
        .put("linger.ms", Long.toString(Durations.toMillis(request.getPublishBatchDuration())))
        .build()
    );
    this.publisher = new KafkaProducer<>(props);
    callbackPool = Executors.newCachedThreadPool();
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, KafkaPublisherTask::new);
  }

  public ListenableFuture<RunResult> doRun() {
    SettableFuture<RunResult> result = SettableFuture.create();
    AtomicInteger messagesToSend = new AtomicInteger(batchSize);
    AtomicInteger messagesSentSuccess = new AtomicInteger(batchSize);
    for (int i = 0; i < batchSize; i++) {
      publisher.send(
          new ProducerRecord<>(topic, null, System.currentTimeMillis(), null, payload),
          (metadata, exception) -> {
            callbackPool.submit(
                new MetricsTask(exception, result, messagesSentSuccess, messagesToSend));
          });
    }
    return result;
  }

  private class MetricsTask implements Runnable {

    private Exception exception;
    private AtomicInteger messagesSentSuccess;
    private AtomicInteger messagesToSend;
    private SettableFuture<RunResult> result;

    public MetricsTask(Exception exception, SettableFuture<RunResult> result,
        AtomicInteger messagesSentSuccess, AtomicInteger messagesToSend) {
      this.exception = exception;
      this.messagesSentSuccess = messagesSentSuccess;
      this.messagesToSend = messagesToSend;
      this.result = result;
    }

    @Override
    public void run() {
      if (exception != null) {
        messagesSentSuccess.decrementAndGet();
        log.error(exception.getMessage(), exception);
      }
      if (messagesToSend.decrementAndGet() == 0) {
        result.set(RunResult.fromBatchSize(messagesSentSuccess.get()));
      }
    }
  }
}
