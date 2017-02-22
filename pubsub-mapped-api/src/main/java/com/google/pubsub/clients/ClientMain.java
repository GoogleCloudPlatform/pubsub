/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.pubsub.clients;

import com.google.common.collect.ImmutableMap;
import com.google.pubsub.clients.producer.PubsubProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Class to test simple features of PubsubProducer and PubsubConsumer.
 */
public class ClientMain {

  private static final Logger log = LoggerFactory.getLogger(ClientMain.class);

  public static void main(String[] args) throws Exception {
    // going to set up the producer and its properties
    String topic = args[0];
    String messageBody = args[1];

    ClientMain main = new ClientMain();
    new Thread(
        new Runnable() {
          public void run() {
            //main.subscriberExample();
          }
        })
        .start();
    Thread.sleep(5000);
    main.publisherExample(topic, messageBody);
  }

  public void publisherExample(String topic, String messageBody) {
    Properties props = new Properties();
    props.putAll(new ImmutableMap.Builder<>()
        .put("project", "dataproc-kafka-test")
        .put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("acks", "all")
        .put("batch.size", 1)
        .put("linger.ms", 1)
        .build()
    );
    Producer publisher = new PubsubProducer<>(props);

    ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, messageBody);

    publisher.send(
        msg,
        new Callback() {
          public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
              log.error("Exception sending the message: " + exception.getMessage());
            } else {
              log.info("Successfully sent message.");
            }
          }
        }
    );
  }
}