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
package com.google.pubsub.flic.kafka;

import com.google.pubsub.flic.common.Utils;
import com.google.pubsub.flic.common.MessagePacketProto.MessagePacket;
import com.google.pubsub.flic.processing.MessageProcessingHandler;
import com.google.pubsub.flic.task.Task;
import com.google.pubsub.flic.task.TaskArgs;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a task that consumes messages utilizing Kafka's implementation of the Consumer<K,V>
 * interface.
 */
public class KafkaConsumerTask extends Task {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumerTask.class.getName());
  private static final String CONSUMER_PROPERTIES = "/consumer.properties";
  private static final long POLL_LENGTH = 1000;

  private KafkaConsumer<String, String> subscriber;
  private MessageProcessingHandler processingHandler;

  public KafkaConsumerTask(
      TaskArgs args,
      KafkaConsumer<String, String> subscriber,
      MessageProcessingHandler processingHandler) {
    super(args);
    this.subscriber = subscriber;
    this.processingHandler = processingHandler;
  }

  public void execute() throws Exception {
    subscriber.subscribe(args.getTopics());
    log.info("Start publishing...");
    long earliestReceived = Long.MAX_VALUE;
    while (messageNo.intValue() <= args.getNumMessages()) {
      ConsumerRecords<String, String> records = subscriber.poll(POLL_LENGTH);
      // Each message in this batch was received at the same time
      long receivedTime = System.currentTimeMillis();
      if (receivedTime < earliestReceived) {
        earliestReceived = receivedTime;
      }
      Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
      while (recordIterator.hasNext() && !failureFlag.get()) {
        ConsumerRecord<String, String> record = recordIterator.next();
        long latency = receivedTime - record.timestamp();
        processingHandler.addStats(1, latency, record.serializedValueSize());
        if (processingHandler.getFiledump() != null) {
          try {
            MessagePacket packet =
                MessagePacket.newBuilder()
                    .setTopic(record.topic())
                    .setKey(record.key())
                    .setValue(record.value())
                    .setLatency(latency)
                    .setReceivedTime(receivedTime)
                    .build();
            processingHandler.addMessagePacket(packet);
          } catch (Exception e) {
            failureFlag.set(true);
            log.error(e.getMessage(), e);
          }
        }
        subscriber.commitAsync();
        MessageProcessingHandler.displayProgress(marker, messageNo);
        if (messageNo.incrementAndGet() > args.getNumMessages()) {
          break;
        }
      }
    }
    if (!failureFlag.get()) {
      subscriber.close();
    }
    processingHandler.printStats(earliestReceived, null, failureFlag);
    log.info("Done!");
  }

  /**
   * Returns a {@link KafkaConsumer} which is initialized with a properties file and a {@link
   * TaskArgs}.
   */
  public static KafkaConsumer<String, String> getInitializedConsumer(TaskArgs args)
      throws Exception {
    Properties props = new Properties();
    InputStream is = Utils.class.getResourceAsStream(CONSUMER_PROPERTIES);
    props.load(is);
    props.put("bootstrap.servers", args.getBroker());
    return new KafkaConsumer<>(props);
  }
}
