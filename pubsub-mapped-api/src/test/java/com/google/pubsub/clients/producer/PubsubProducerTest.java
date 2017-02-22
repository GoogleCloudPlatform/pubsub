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
package com.google.kafka.clients.producer;

/*import org.apache.kafka.clients.CommonClientConfigs;
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
import java.util.Properties;*/

//@RunWith(PowerMockRunner.class)
//@PowerMockIgnore("javax.management.*")
public class PubsubProducerTest {

   /* @Test
    public void testSerializerClose() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        configs.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL);
        final int oldInitCount = MockSerializer.INIT_COUNT.get();
        final int oldCloseCount = MockSerializer.CLOSE_COUNT.get();

        PubsubProducer<byte[], byte[]> producer = new PubsubProducer<byte[], byte[]>(
                configs, new MockSerializer(), new MockSerializer());
        Assert.assertEquals(oldInitCount + 2, MockSerializer.INIT_COUNT.get());
        Assert.assertEquals(oldCloseCount, MockSerializer.CLOSE_COUNT.get());

        producer.close();
        Assert.assertEquals(oldInitCount + 2, MockSerializer.INIT_COUNT.get());
        Assert.assertEquals(oldCloseCount + 2, MockSerializer.CLOSE_COUNT.get());
    }

    @Test
    public void testInterceptorConstructClose() throws Exception {
        try {
            Properties props = new Properties();
            // test with client ID assigned by PubsubProducer
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
            props.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MockProducerInterceptor.class.getName());
            props.setProperty(MockProducerInterceptor.APPEND_STRING_PROP, "something");

            PubsubProducer<String, String> producer = new PubsubProducer<String, String>(
                    props, new StringSerializer(), new StringSerializer());
            Assert.assertEquals(1, MockProducerInterceptor.INIT_COUNT.get());
            Assert.assertEquals(0, MockProducerInterceptor.CLOSE_COUNT.get());

            // Cluster metadata will only be updated on calling onSend.
            Assert.assertNull(MockProducerInterceptor.CLUSTER_META.get());

            producer.close();
            Assert.assertEquals(1, MockProducerInterceptor.INIT_COUNT.get());
            Assert.assertEquals(1, MockProducerInterceptor.CLOSE_COUNT.get());
        } finally {
            // cleanup since we are using mutable static variables in MockProducerInterceptor
            MockProducerInterceptor.resetCounters();
        }
    }

    @Test
    public void testOsDefaultSocketBufferSizes() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ProducerConfig.SEND_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, Selectable.USE_DEFAULT_BUFFER_SIZE);
        PubsubProducer<byte[], byte[]> producer = new PubsubProducer<>(
                config, new ByteArraySerializer(), new ByteArraySerializer());
        producer.close();
    }

    @Test(expected = KafkaException.class)
    public void testInvalidSocketSendBufferSize() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ProducerConfig.SEND_BUFFER_CONFIG, -2);
        new PubsubProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Test(expected = KafkaException.class)
    public void testInvalidSocketReceiveBufferSize() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
        config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, -2);
        new PubsubProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    }
    */
}
