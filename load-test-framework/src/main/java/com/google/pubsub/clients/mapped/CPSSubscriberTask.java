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

import com.google.protobuf.util.Durations;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.consumer.KafkaConsumer;
import com.google.pubsub.clients.common.MetricsHandler.MetricName;

import com.google.pubsub.flic.common.LoadtestProto.StartRequest;

import java.util.concurrent.Semaphore;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Properties;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Runs a task that consumes messages utilizing Pub/Sub's implementation of the Kafka Consumer<K,V> Interface.
 */
class CPSSubscriberTask extends Task {

  private static final Logger log = LoggerFactory.getLogger(CPSSubscriberTask.class);

  private final long pollLength;
  private final Semaphore outstandingPolls;
  private final KafkaConsumer<String, String> subscriber;


  private CPSSubscriberTask(StartRequest request) {
    super(request, "mapped", MetricName.END_TO_END_LATENCY);

    Properties props = new Properties();
    //DONT CHANGE, ELSE YOU WILL BREAK THE TEST.
    props.put("group.id", "test");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    this.subscriber = new KafkaConsumer<>(props);
    this.subscriber.subscribe(Collections.singletonList(request.getTopic()));
    this.outstandingPolls = new Semaphore(request.getMaxOutstandingRequests());
    this.pollLength = Durations.toMillis(request.getKafkaOptions().getPollDuration());
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, CPSSubscriberTask::new);
  }

  @Override
  public ListenableFuture<RunResult> doRun() {
    if (!outstandingPolls.tryAcquire()) {
      return Futures.immediateFailedFuture(new Exception("Flow control limits reached."));
    }
    ConsumerRecords<String, String> records = subscriber.poll(300000);
    records.forEach(
        record -> {
          String[] tokens = record.key().split("#");
          recordMessageLatency(
              Integer.parseInt(tokens[0]),
              Integer.parseInt(tokens[2]),
              System.currentTimeMillis() - Long.parseLong(tokens[1]));
        });
    outstandingPolls.release();
    return Futures.immediateFuture(RunResult.empty());
  }

  @Override
  public void shutdown() {
    //subscriber.close();
  }
}
