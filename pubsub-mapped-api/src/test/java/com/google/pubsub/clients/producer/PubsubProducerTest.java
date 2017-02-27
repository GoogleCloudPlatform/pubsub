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
/*package com.google.kafka.clients.producer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.MockProducerInterceptor;
import org.apache.kafka.test.MockSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class PubsubProducerTest {

  @Test
  public void testConstructorWithSerializers() {
    Properties props = new Properties();
    props.put(PubsubProducerConfig.PROJECT_CONFIG, "dataproc-kafka-test");
    new PubsubProducer(props, new ByteArraySerializer(), new ByteArraySerializer()).close();
  }

  @Test(expected = ConfigException.class)
  public void testNoSerializerProvided() {
    Properties props = new Properties();
    props.put(PubsubProducerConfig.PROJECT_CONFIG, "dataproc-kafka-test");
    new PubsubProducer(props);
  }


}*/
