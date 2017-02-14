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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.LogEntry;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.After;
import org.junit.Test;

public class PubsubAccumulatorTest {

    private String topic = "test-topic";
    private MockTime time = new MockTime();
    private SystemTime systemTime = new SystemTime();
    private byte[] key = "key".getBytes();
    private byte[] value = "value".getBytes();
    private int msgSize = Records.LOG_OVERHEAD + Record.recordSize(key, value);
    private Metrics metrics = new Metrics(time);
    private final long maxBlockTimeMs = 1000;

    @After
    public void teardown() {
        this.metrics.close();
    }

    @Test
    public void testFull() throws Exception {
        long now = time.milliseconds();
        int batchSize = 1024;
        PubsubAccumulator accum = new PubsubAccumulator(batchSize, 10 * batchSize, CompressionType.NONE, 10L, 100L, metrics, time);
        int appends = batchSize / msgSize;
        for (int i = 0; i < appends; i++) {
            // append to the first batch
            accum.append(topic, 0L, key, value, null, maxBlockTimeMs);
            Deque<PubsubBatch> batches = accum.batches().get(topic);
            assertEquals(1, batches.size());
            assertTrue(batches.peekFirst().records.isWritable());
            assertEquals("No topics should be ready.", 0, accum.ready(now).size());
        }

        // this append doesn't fit in the first batch, so a new batch is created and the first batch is closed
        accum.append(topic, 0L, key, value, null, maxBlockTimeMs);
        Deque<PubsubBatch> allBatches = accum.batches().get(topic);
        assertEquals(2, allBatches.size());
        Iterator<PubsubBatch> batchesIterator = allBatches.iterator();
        assertFalse(batchesIterator.next().records.isWritable());
        assertTrue(batchesIterator.next().records.isWritable());
        assertEquals("Topic should be ready.", Collections.singleton(topic), accum.ready(time.milliseconds()));

        List<PubsubBatch> batches = accum.drain(Collections.singleton(topic), Integer.MAX_VALUE, 0).get(topic);
        assertEquals(1, batches.size());
        PubsubBatch batch = batches.get(0);

        Iterator<LogEntry> iter = batch.records.iterator();
        for (int i = 0; i < appends; i++) {
            LogEntry entry = iter.next();
            assertEquals("Keys should match", ByteBuffer.wrap(key), entry.record().key());
            assertEquals("Values should match", ByteBuffer.wrap(value), entry.record().value());
        }
        assertFalse("No more records", iter.hasNext());
    }

    @Test
    public void testAppendLarge() throws Exception {
        int batchSize = 512;
        PubsubAccumulator accum = new PubsubAccumulator(batchSize, 10 * 1024, CompressionType.NONE, 0L, 100L, metrics, time);
        accum.append(topic, 0L, key, new byte[2 * batchSize], null, maxBlockTimeMs);
        assertEquals("Topic should be ready", Collections.singleton(topic), accum.ready(time.milliseconds()));
    }

    @Test
    public void testLinger() throws Exception {
        long lingerMs = 10L;
        PubsubAccumulator accum = new PubsubAccumulator(1024, 10 * 1024, CompressionType.NONE, lingerMs, 100L, metrics, time);
        accum.append(topic, 0L, key, value, null, maxBlockTimeMs);
        assertEquals("No partitions should be ready", 0, accum.ready(time.milliseconds()).size());
        time.sleep(10);
        assertEquals("Our partition's leader should be ready", Collections.singleton(topic), accum.ready(time.milliseconds()));
        List<PubsubBatch> batches = accum.drain(Collections.singleton(topic), Integer.MAX_VALUE, 0).get(topic);
        assertEquals(1, batches.size());
        PubsubBatch batch = batches.get(0);

        Iterator<LogEntry> iter = batch.records.iterator();
        LogEntry entry = iter.next();
        assertEquals("Keys should match", ByteBuffer.wrap(key), entry.record().key());
        assertEquals("Values should match", ByteBuffer.wrap(value), entry.record().value());
        assertFalse("No more records", iter.hasNext());
    }

    @Test
    public void testPartialDrain() throws Exception {
        PubsubAccumulator accum = new PubsubAccumulator(1024, 10 * 1024, CompressionType.NONE, 10L, 100L, metrics, time);
        int appends = 1024 / msgSize + 1;
        for (int i = 0; i < appends; i++) {
            accum.append(topic, 0L, key, value, null, maxBlockTimeMs);
        }
        assertEquals("Partition's leader should be ready", Collections.singleton(topic), accum.ready(time.milliseconds()));

        List<PubsubBatch> batches = accum.drain(Collections.singleton(topic), 1024, 0).get(topic);
        assertEquals("But due to size bound only one partition should have been retrieved", 1, batches.size());
    }

    @SuppressWarnings("unused")
    @Test
    public void testStressfulSituation() throws Exception {
        final int numThreads = 5;
        final int msgs = 10000;
        final int numParts = 2;
        final PubsubAccumulator accum = new PubsubAccumulator(1024, 10 * 1024, CompressionType.NONE, 0L, 100L, metrics, time);
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < numThreads; i++) {
            threads.add(new Thread() {
                public void run() {
                    for (int i = 0; i < msgs; i++) {
                        try {
                            accum.append(topic, 0L, key, value, null, maxBlockTimeMs);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        for (Thread t : threads)
            t.start();
        int read = 0;
        long now = time.milliseconds();
        while (read < numThreads * msgs) {
            Set<String> readyTopics = accum.ready(now);
            List<PubsubBatch> batches = accum.drain(readyTopics, 5 * 1024, 0).get(topic);
            if (batches != null) {
                for (PubsubBatch batch : batches) {
                    for (LogEntry entry : batch.records)
                        read++;
                    accum.deallocate(batch);
                }
            }
        }

        for (Thread t : threads)
            t.join();
    }

    @Test
    public void testNextReadyCheckDelay() throws Exception {
        // Next check time will use lingerMs since this test won't trigger any retries/backoff
        long lingerMs = 10L;
        PubsubAccumulator accum = new PubsubAccumulator(1024, 10 * 1024,  CompressionType.NONE, lingerMs, 100L, metrics, time);
        // Just short of going over the limit so we trigger linger time
        int appends = 512 / msgSize; // Gets through 2 attempted sends without filling batch

        for (int i = 0; i < appends; i++)
            accum.append(topic, 0L, key, value, null, maxBlockTimeMs);
        Set<String> readyNodes = accum.ready(time.milliseconds());
        assertEquals("No topics should be ready.", 0, readyNodes.size());

        time.sleep(lingerMs / 2);

        for (int i = 0; i < appends; i++)
            accum.append(topic, 0L, key, value, null, maxBlockTimeMs);
        readyNodes = accum.ready(time.milliseconds());
        assertEquals("No topics should be ready.", 0, readyNodes.size());

        // Add enough to make data sendable immediately
        for (int i = 0; i < appends + 1; i++)
            accum.append(topic, 0L, key, value, null, maxBlockTimeMs);
        readyNodes = accum.ready(time.milliseconds());
        assertEquals("Topic should be ready", Collections.singleton(topic), readyNodes);
    }

    @Test
    public void testRetryBackoff() throws Exception {
        long lingerMs = Long.MAX_VALUE / 4;
        long retryBackoffMs = Long.MAX_VALUE / 2;
        final PubsubAccumulator accum = new PubsubAccumulator(1024, 10 * 1024, CompressionType.NONE, lingerMs, retryBackoffMs, metrics, time);

        long now = time.milliseconds();
        accum.append(topic, 0L, key, value, null, maxBlockTimeMs);
        Set<String> readyTopics = accum.ready(now + lingerMs + 1);
        assertEquals("Topic1 should be ready", Collections.singleton(topic), readyTopics);
        Map<String, List<PubsubBatch>> batches = accum.drain(readyTopics, Integer.MAX_VALUE, now + lingerMs + 1);
        assertEquals("Topic1 should be the only ready topic.", 1, batches.size());
        assertEquals("There should only be one batch drained.", 1, batches.get(topic).size());

        // Reenqueue the batch
        now = time.milliseconds();
        accum.reenqueue(batches.get(topic).get(0), now);

        // Put another message into accumulator
        accum.append(topic, 0L, key, value, null, maxBlockTimeMs);
        readyTopics = accum.ready(now + lingerMs + 1);
        assertEquals("Topic should not be ready due to backoff", 0, readyTopics.size());

        // topic though backoff, should drain both batches
        readyTopics = accum.ready(now + retryBackoffMs + 1);
        batches = accum.drain(readyTopics, Integer.MAX_VALUE, now + retryBackoffMs + 1);
        assertEquals("Topic should drain the first batch.", 1, batches.get(topic).size());
        readyTopics = accum.ready(now + retryBackoffMs + 1);
        batches = accum.drain(readyTopics, Integer.MAX_VALUE, now + retryBackoffMs + 1);
        assertEquals("Topic should drain the second batch.", 1, batches.get(topic).size());
    }

    @Test
    public void testFlush() throws Exception {
        long lingerMs = Long.MAX_VALUE;
        int batchSize = 4 * 1024;
        final PubsubAccumulator accum = new PubsubAccumulator(batchSize, 64 * 1024, CompressionType.NONE, lingerMs, 100L, metrics, time);
        int attempts = batchSize / msgSize;
        for (int i = 0; i < attempts; i++)
            accum.append(topic, 0L, key, value, null, maxBlockTimeMs); // Fill almost 1 complete batch
        Set<String> readyTopics = accum.ready(time.milliseconds());
        assertEquals("No topics should be ready.", 0, readyTopics.size());

        accum.beginFlush();
        readyTopics = accum.ready(time.milliseconds());

        // drain and deallocate all batches
        Map<String, List<PubsubBatch>> results = accum.drain(readyTopics, Integer.MAX_VALUE, time.milliseconds());
        for (List<PubsubBatch> batches: results.values())
            for (PubsubBatch batch: batches)
                accum.deallocate(batch);

        // should be complete with no unsent records.
        accum.awaitFlushCompletion();
        assertFalse(accum.hasUnsent());
    }

    private void delayedInterrupt(final Thread thread, final long delayMs) {
        Thread t = new Thread() {
            public void run() {
                systemTime.sleep(delayMs);
                thread.interrupt();
            }
        };
        t.start();
    }

    @Test
    public void testAwaitFlushComplete() throws Exception {
        PubsubAccumulator accum = new PubsubAccumulator(4 * 1024, 64 * 1024, CompressionType.NONE, Long.MAX_VALUE, 100L, metrics, time);
        accum.append(topic, 0L, key, value, null, maxBlockTimeMs);

        accum.beginFlush();
        assertTrue(accum.flushInProgress());
        delayedInterrupt(Thread.currentThread(), 1000L);
        try {
            accum.awaitFlushCompletion();
            fail("awaitFlushCompletion should throw InterruptException");
        } catch (InterruptedException e) {
            assertFalse("flushInProgress count should be decremented even if thread is interrupted", accum.flushInProgress());
        }
    }

    @Test
    public void testAbortIncompleteBatches() throws Exception {
        long lingerMs = Long.MAX_VALUE;
        int batchSize = 4 * 1024;
        final AtomicInteger numExceptionReceivedInCallback = new AtomicInteger(0);
        final PubsubAccumulator accum = new PubsubAccumulator(batchSize, 64 * 1024, CompressionType.NONE, lingerMs, 100L, metrics, time);
        class TestCallback implements Callback {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                assertTrue(exception.getMessage().equals("Producer is closed forcefully."));
                numExceptionReceivedInCallback.incrementAndGet();
            }
        }
        int attempts = batchSize / msgSize;
        for (int i = 0; i < attempts; i++)
            accum.append(topic, 0L, key, value, new TestCallback(), maxBlockTimeMs);
        Set<String> readyTopics = accum.ready(time.milliseconds());
        assertEquals("No topics should be ready.", 0, readyTopics.size());

        accum.abortIncompleteBatches();
        assertEquals(numExceptionReceivedInCallback.get(), attempts);
        assertFalse(accum.hasUnsent());

    }

    @Test
    public void testExpiredBatches() throws InterruptedException {
        long retryBackoffMs = 100L;
        long lingerMs = 3000L;
        int batchSize = 1024;
        int requestTimeout = 60;

        PubsubAccumulator accum = new PubsubAccumulator(batchSize, 10 * 1024, CompressionType.NONE, lingerMs, retryBackoffMs, metrics, time);
        int appends = batchSize / msgSize;

        // Test batches not in retry
        for (int i = 0; i < appends; i++) {
            accum.append(topic, 0L, key, value, null, maxBlockTimeMs);
            assertEquals("No topics should be ready.", 0, accum.ready(time.milliseconds()).size());
        }
        // Make the batches ready due to batch full
        accum.append(topic, 0L, key, value, null, 0);
        Set<String> readyTopics = accum.ready(time.milliseconds());
        assertEquals("Topic should be ready", Collections.singleton(topic), readyTopics);
        // Advance the clock to expire the batch.
        time.sleep(requestTimeout + 1);
        accum.muteTopic(topic);
        List<PubsubBatch> expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch should not be expired when the topic is muted", 0, expiredBatches.size());

        accum.unmuteTopic(topic);
        expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch should be expired", 1, expiredBatches.size());
        assertEquals("No topics should be ready.", 0, accum.ready(time.milliseconds()).size());

        // Advance the clock to make the next batch ready due to linger.ms
        time.sleep(lingerMs);
        assertEquals("Topic should be ready", Collections.singleton(topic), readyTopics);
        time.sleep(requestTimeout + 1);

        accum.muteTopic(topic);
        expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch should not be expired when topic is muted", 0, expiredBatches.size());

        accum.unmuteTopic(topic);
        expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch should be expired when the topic is not muted", 1, expiredBatches.size());
        assertEquals("No topics should be ready.", 0, accum.ready(time.milliseconds()).size());

        // Test batches in retry.
        // Create a retried batch
        accum.append(topic, 0L, key, value, null, 0);
        time.sleep(lingerMs);
        readyTopics = accum.ready(time.milliseconds());
        assertEquals("Our partition's leader should be ready", Collections.singleton(topic), readyTopics);
        Map<String, List<PubsubBatch>> drained = accum.drain(readyTopics, Integer.MAX_VALUE, time.milliseconds());
        assertEquals("There should be only one batch.", drained.get(topic).size(), 1);
        time.sleep(1000L);
        accum.reenqueue(drained.get(topic).get(0), time.milliseconds());

        // test expiration.
        time.sleep(requestTimeout + retryBackoffMs);
        expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch should not be expired.", 0, expiredBatches.size());
        time.sleep(1L);

        accum.muteTopic(topic);
        expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch should not be expired when the topic is muted", 0, expiredBatches.size());

        accum.unmuteTopic(topic);
        expiredBatches = accum.abortExpiredBatches(requestTimeout, time.milliseconds());
        assertEquals("The batch should be expired when the topic is not muted.", 1, expiredBatches.size());
    }

    @Test
    public void testMutedPartitions() throws InterruptedException {
        long now = time.milliseconds();
        PubsubAccumulator accum = new PubsubAccumulator(1024, 10 * 1024, CompressionType.NONE, 10, 100L, metrics, time);
        int appends = 1024 / msgSize;
        for (int i = 0; i < appends; i++) {
            accum.append(topic, 0L, key, value, null, maxBlockTimeMs);
            assertEquals("No partitions should be ready.", 0, accum.ready(now).size());
        }
        time.sleep(2000);

        // Test ready with muted partition
        accum.muteTopic(topic);
        Set<String> readyTopics = accum.ready(time.milliseconds());
        assertEquals("No node should be ready", 0, readyTopics.size());

        // Test ready without muted partition
        accum.unmuteTopic(topic);
        readyTopics = accum.ready(time.milliseconds());
        assertTrue("The batch should be ready", readyTopics.size() > 0);

        // Test drain with muted partition
        accum.muteTopic(topic);
        Map<String, List<PubsubBatch>> drained = accum.drain(readyTopics, Integer.MAX_VALUE, time.milliseconds());
        assertEquals("No batch should have been drained", 0, drained.get(topic).size());

        // Test drain without muted partition.
        accum.unmuteTopic(topic);
        drained = accum.drain(readyTopics, Integer.MAX_VALUE, time.milliseconds());
        assertTrue("The batch should have been drained.", drained.get(topic).size() > 0);
    }
}
