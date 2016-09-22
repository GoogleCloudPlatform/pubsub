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
import com.google.pubsub.flic.processing.MessageProcessingHandler;
import com.google.pubsub.flic.task.Task;
import com.google.pubsub.flic.task.TaskArgs;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a task that publishes messages utilizing Kafka's implementation of the Producer<K,V>
 * interface
 */
public class KafkaPublishingTask extends Task {

  private static final Logger log = LoggerFactory.getLogger(KafkaPublishingTask.class.getName());
  private static final String PRODUCER_PROPERTIES = "/producer.properties";

  private KafkaProducer<String, String> publisher;
//  private MessageProcessingHandler processingHandler;

  public KafkaPublishingTask(
      TaskArgs args, KafkaProducer<String, String> publisher, MessageProcessingHandler processingHandler) {
    super(args);
    this.publisher = publisher;
//    this.processingHandler = processingHandler;
  }

  public void execute() throws Exception {
    List<String> topics = args.getTopics();
    String baseMessage = Utils.createMessage(args.getMessageSize(), 0);
    // Keep track of the number of bytes sent and number of messages
    AtomicLong sentBytes = new AtomicLong(0);
    AtomicInteger counter = new AtomicInteger(1);
    long start = System.currentTimeMillis();
    log.info("Start: " + start);
    while (messageNo.intValue() <= args.getNumMessages() && !failureFlag.get()) {
      String messageToSend = baseMessage + messageNo;
      ProducerRecord<String, String> record =
          new ProducerRecord<>(
              topics.get(messageNo.intValue() % topics.size()),
              null,
              System.currentTimeMillis(),
              String.valueOf(messageNo),
              messageToSend);
      sentBytes.addAndGet(messageToSend.getBytes().length);
      publisher.send(
          record,
          new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
              if (!failureFlag.get()) {
                if (exception != null) {
                  log.error(exception.getMessage(), exception);
                  failureFlag.set(true);
                }
                long latency = System.currentTimeMillis() - metadata.timestamp();
                log.info("Latency: " + latency);
//                processingHandler.addStats(counter.intValue() - 1, latency, sentBytes.longValue());
              }
            }
          });
      MessageProcessingHandler.displayProgress(marker, messageNo);
      messageNo.incrementAndGet();
    }
    if (!failureFlag.get()) {
      log.info("Waiting for all acks to arrive...");
      publisher.flush();
      log.info("Other size of flush");
    }
    log.info("Printing stats");
//    processingHandler.printStats(start, null, failureFlag);
    log.info("Done!");
  }

  /**
   * Returns a {@link KafkaProducer} which is initialized with a properties file and a {@link
   * TaskArgs}.
   */
  public static KafkaProducer<String, String> getInitializedProducer(TaskArgs args)
      throws Exception {
    Properties props = new Properties();
    InputStream is = Utils.class.getResourceAsStream(PRODUCER_PROPERTIES);
    props.load(is);
    props.put("bootstrap.servers", args.getBroker());
    return new KafkaProducer<>(props);
  }
}
