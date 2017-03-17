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

package com.google.pubsub.clients.tests;

import com.google.pubsub.clients.producer.PubsubProducer;
import com.google.pubsub.clients.producer.PubsubProducer.Builder;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Creates a thread that sends a given series of messages. Serves as a sanity check for the
 * PubsubProducer implementation.
 */
public class ProducerThread implements Runnable {
  private String command;
  private PubsubProducer producer;
  private String topic;
  private static final Logger log = LoggerFactory.getLogger(ProducerThread.class);
  private int numMessages;

  public ProducerThread(String s, Properties props, String topic, int numMessages) {
    this.command = s;
    this.producer = new Builder<>(props.getProperty("project"), topic, new StringSerializer(), new StringSerializer())
        .batchSize(Integer.parseInt(props.getProperty("batch.size")))
        .isAcks(props.getProperty("acks").matches("1|all"))
        .lingerMs(Long.parseLong(props.getProperty("linger.ms")))
    .build();
    this.topic = topic;
    this.numMessages = numMessages;
  }

  public void run() {
    log.info("Start running the command");
    processCommand();
    log.info("End running the command");
  }

  private void processCommand() {
    try {
      for (int i = 0; i < numMessages; i++) {
        ProducerRecord<String, String> msg = new ProducerRecord<>(topic, "hello" + command + i);
        producer.send(
            msg,
            new Callback() {
              public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                  log.error("Exception sending the message: " + exception.getMessage());
                } else {
                  log.info("Successfully sent message");
                }
              }
            }
        );
      }
      Thread.sleep(5000);
      producer.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public String toString() {
    return command;
  }
}