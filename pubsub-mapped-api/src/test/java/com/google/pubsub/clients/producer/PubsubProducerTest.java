/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.pubsub.clients.producer;

import com.google.common.collect.ImmutableMap;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.config.ConfigException;


import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubsubProducerTest {

  private static final String TOPIC = "testTopic";
  private static final String MESSAGE = "testMessage";

  /* Constructor Tests */
  @Test
  public void testConstructorWithSerializers() {
    Properties props = new Properties();
    props.put(PubsubProducerConfig.PROJECT_CONFIG, "dataproc-kafka-test");
    new PubsubProducer(props, new ByteArraySerializer(), new ByteArraySerializer()).close();
  }

  @Test(expected = ConfigException.class)
  public void testConstructorNoSerializerProvided() {
    Properties props = new Properties();
    props.setProperty(PubsubProducerConfig.PROJECT_CONFIG, "dataproc-kafka-test");
    new PubsubProducer(props).close();
  }

  @Test(expected = ConfigException.class)
  public void testConstructorNoProjectProvided() {
    Properties props = new Properties();
    props.putAll(new ImmutableMap.Builder<>()
        .put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .build()
    );
    new PubsubProducer(props).close();
  }

  /* send() tests */
 /* @Test(expected = RuntimeException.class)
  public void testSendPublisherClosed() {

  }*/

  private PubsubProducer getNewProducer() {
    Properties props = new Properties();
    props.putAll(new ImmutableMap.Builder<>()
        .put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("project", "dataproc-kafka-test")
        .build()
    );

    return new PubsubProducer(props);
  }

}
