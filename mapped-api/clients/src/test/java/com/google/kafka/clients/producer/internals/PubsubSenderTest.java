/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.google.kafka.clients.producer.internals;

import com.google.pubsub.v1.PublishResponse;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.MockTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PubsubSenderTest {

    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final short ACKS_ALL = -1;
    private static final int MAX_RETRIES = 0;
    private static final String CLIENT_ID = "clientId";
    private static final String METRIC_GROUP = "producer-metrics";
    private static final double EPS = 0.0001;
    private static final int MAX_BLOCK_TIMEOUT = 1000;
    private static final int REQUEST_TIMEOUT = 10000;

    private String topic = "test-topic";
    private MockTime time = new MockTime();
    private int batchSize = 16 * 1024;
    private Metrics metrics = null;
    private PubsubAccumulator accumulator = null;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(15);

    @Before
    public void setup() {
        Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("client-id", CLIENT_ID);
        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        metrics = new Metrics(metricConfig, time);
        accumulator = new PubsubAccumulator(batchSize, 1024 * 1024, CompressionType.NONE, 0L, 0L, metrics, time);
    }

    @After
    public void tearDown() {
        this.metrics.close();
    }

    @Test
    public void testSimple() throws Exception {
        MockPubsubServer server = newServer("testSimple");
        PubsubSender sender = newSender("testSimple", MAX_RETRIES);
        long offset = 32;
        Future<RecordMetadata> future = accumulator.append(topic, 0L, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds()); // Sends produce request
        assertTrue("Server should receive request.", server.listen(1, 1000));
        server.respond(PublishResponse.newBuilder().addMessageIds(Long.toString(offset)).build());
        assertEquals("All requests completed.", 0, (long) server.inFlightCount());
        assertNotNull("Request should be completed", future.get());
        waitForUnmute(topic, 1000);
    }

    @Test
    public void testRetries() throws Exception {
        int maxRetries = 1;
        MockPubsubServer server = newServer("testRetries");
        PubsubSender sender = newSender("testRetries", maxRetries);
        // do a successful retry
        Future<RecordMetadata> future = accumulator.append(topic, 0L, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds()); // send produce request
        assertTrue("Server should receive request.", server.listen(1, 1000));
        server.disconnect();
        assertEquals("All requests completed.", 0, server.inFlightCount());
        completedWithError(future, Errors.NETWORK_EXCEPTION);
        waitForUnmute(topic, 1000);
        sender.run(time.milliseconds()); // send second produce request
        assertTrue("Server should receive request..", server.listen(1, 1000));
        long offset = 32;
        server.respond(PublishResponse.newBuilder().addMessageIds(Long.toString(offset)).build());
        assertEquals("All requests completed.", 0, (long) server.inFlightCount());
        eventualReturn(future, 1000);
        assertEquals(offset, future.get().offset());
        waitForUnmute(topic, 1000);

        // do an unsuccessful retry
        future = accumulator.append(topic, 0L, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
        for (int i = 0; i < maxRetries + 1; i++) {
            sender.run(time.milliseconds()); // send produce request
            assertTrue("Server should receive request.", server.listen(1, 1000));
            server.disconnect();
            completedWithError(future, Errors.NETWORK_EXCEPTION);
            waitForUnmute(topic, 1000);
        }
        sender.run(time.milliseconds());
        assertEquals("Retry request should be received.", 0, server.inFlightCount());
        waitForUnmute(topic, 1000);
    }

    @Test
    public void testSendInOrder() throws Exception {
        PubsubSender sender = newSender("testSendInOrder", MAX_RETRIES);
        MockPubsubServer server = newServer("testSendInOrder");

        // Send the first message.
        accumulator.append(topic, 0L, "key1".getBytes(), "value1".getBytes(), null, MAX_BLOCK_TIMEOUT);
        sender.run(time.milliseconds()); // send produce request
        assertTrue("Server should receive request.", server.listen(1, 1000));

        time.sleep(900);
        // Now send another message
        accumulator.append(topic, 0L, "key2".getBytes(), "value2".getBytes(), null, MAX_BLOCK_TIMEOUT);

        // Sender should not send second message before first is returned
        sender.run(time.milliseconds());
        assertTrue("Server expects only one request.", server.listen(1, 1000));
    }

    private void completedWithError(Future<RecordMetadata> future, Errors error) throws Exception {
        try {
            future.get();
            fail("Should have thrown an exception.");
        } catch (ExecutionException e) {
            assertEquals(error.exception().getClass(), e.getCause().getClass());
        }
    }

    private void eventualReturn(Future<RecordMetadata> future, long waitMillis) {
        for (int i = 0; i < waitMillis / 50; i++) {
            try {
                if (future.get() != null) {
                    return;
                } else {
                    break;
                }
            } catch (ExecutionException | InterruptedException e) { } // Ignore and wait a turn
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                i--; // Not a big deal to be interrupted, just go another time through the loop
            }
        }
        fail("Should have received a non-null result from future without exception");
    }

    private void waitForUnmute(String topic, long waitMillis) {
        for (int i = 0; i < waitMillis / 50; i++) {
            if (!accumulator.isMutedTopic(topic)) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) { }
        }
        fail(topic + " was never unmuted.");
    }

    private PubsubSender newSender(String channelName, int retries) {
        return new PubsubSender(InProcessChannelBuilder.forName(channelName).directExecutor().build(),
                this.accumulator,
                true,
                MAX_REQUEST_SIZE,
                retries,
                metrics,
                time,
                REQUEST_TIMEOUT);
    }

    private MockPubsubServer newServer(String channelName) {
        MockPubsubServer out = new MockPubsubServer();
        try {
            InProcessServerBuilder.forName(channelName).directExecutor().addService(out).build().start();
        } catch (IOException e) {
            return null;
        }
        return out;
    }

//    private ProduceResponse produceResponse(TopicPartition tp, long offset, int error, int throttleTimeMs) {
//        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse((short) error, offset, Record.NO_TIMESTAMP);
//        Map<TopicPartition, ProduceResponse.PartitionResponse> partResp = Collections.singletonMap(tp, resp);
//        return new ProduceResponse(partResp, throttleTimeMs);
//    }

}
