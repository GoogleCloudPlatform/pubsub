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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.google.pubsub.clients.common.Task;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.consumer.KafkaConsumer;
import com.google.pubsub.clients.common.MetricsHandler.MetricName;

import com.google.pubsub.flic.common.LoadtestProto.StartRequest;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Properties;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *  Runs a task that consumes messages utilizing Pub/Sub's implementation of the Kafka Consumer<K,V> Interface.
 */
class CPSSubscriberTask extends Task {

  private static final long POLL_TIMEOUT = 300000L;
  private final KafkaConsumer<String, String> subscriber;
  private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

  private CPSSubscriberTask(StartRequest request) {
    super(request, "mapped", MetricName.END_TO_END_LATENCY);

    Properties props = new Properties();

    //TODO: DON'T CHANGE, ELSE YOU WILL BREAK THE TEST. Subscription name depends on it.
    props.put("group.id", "SUBSCRIBER_ID");
    props.put("enable.auto.commit", "false");
    props.put("project", request.getProject());
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    this.subscriber = new KafkaConsumer<>(props);
    this.subscriber.subscribe(Collections.singletonList(request.getTopic()));
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, CPSSubscriberTask::new);
  }

  @Override
  public ListenableFuture<RunResult> doRun() {
    if (shuttingDown.get()) {
      return Futures.immediateFailedFuture(
          new IllegalStateException("The task is shutting down."));
    }
    ConsumerRecords<String, String> records = null;
    try {
      records = subscriber.poll(POLL_TIMEOUT);
    } catch (KafkaException e) {
      return Futures.immediateFailedFuture(
          new IllegalStateException("The task is shut down."));
    }
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
    subscriber.commitAsync();
    return Futures.immediateFuture(RunResult.empty());
  }

  @Override
  public void shutdown() {
    if (shuttingDown.getAndSet(true)) {
      return;
    }
    subscriber.close();
  }
}
