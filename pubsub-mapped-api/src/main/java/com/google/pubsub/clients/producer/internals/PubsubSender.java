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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The background thread that handles the sending of produce requests to Cloud Pub/Sub. This thread makes metadata
 * requests to renew its view of the cluster and then sends produce requests to the appropriate nodes.
 */
public class PubsubSender implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Sender.class);

    /* the record accumulator that batches records */
    private final PubsubAccumulator accumulator;

    /* the grpc stub to send records to pubsub */
    private final PublisherGrpc.PublisherFutureStub stub;

    private final ThreadPoolExecutor executor;

    /* the flag indicating whether the producer should guarantee the message order on the broker or not. */
    private final boolean guaranteeMessageOrder;

    /* the maximum request size to attempt to send to the server */
    private final int maxRequestSize;

    /* the number of times to retry a failed request before giving up */
    private final int retries;

    /* the clock instance used for getting the time */
    private final Time time;

    /* true while the sender thread is still running */
    private volatile boolean running;

    /* true when the caller wants to ignore all unsent/inflight messages and force close.  */
    private volatile boolean forceClose;

    /* metrics */
    private final PubsubSenderMetrics sensors;

    /* the max time to wait for the server to respond to the request*/
    private final int requestTimeout;

    public PubsubSender(ManagedChannel channel,
                        PubsubAccumulator accumulator,
                        boolean guaranteeMessageOrder,
                        int maxRequestSize,
                        int retries,
                        Metrics metrics,
                        Time time,
                        int requestTimeout) {
        this.accumulator = accumulator;
        this.guaranteeMessageOrder = guaranteeMessageOrder;
        this.maxRequestSize = maxRequestSize;
        this.running = true;
        this.retries = retries;
        this.time = time;
        this.sensors = new PubsubSenderMetrics(metrics);
        this.requestTimeout = requestTimeout;
        this.stub = PublisherGrpc.newFutureStub(channel)
                .withDeadlineAfter(requestTimeout, TimeUnit.MILLISECONDS);
        this.executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>());
    }

    @Override
    public void run() {
        log.debug("Starting Kafka producer I/O thread.");

        // main loop, runs until close is called
        while (running) {
            try {
                run(time.milliseconds());
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

        // okay we stopped accepting requests but there may still be
        // requests in the accumulator or waiting for acknowledgment,
        // wait until these are completed.
        while (!forceClose && (this.accumulator.hasUnsent() || this.executor.getActiveCount() > 0)) {
            try {
                run(time.milliseconds());
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }
        if (forceClose) {
            // We need to fail all the incomplete batches and wake up the threads waiting on
            // the futures.
            this.accumulator.abortIncompleteBatches();
        }
        try {
            this.executor.shutdown();
        } catch (Exception e) {
            log.error("Failed to close network client", e);
        }

        log.debug("Shutdown of Kafka producer I/O thread has completed.");
    }

    /**
     * Run a single iteration of sending
     *
     * @param now
     *            The current POSIX time in milliseconds
     */
    void run(long now) {
        Set<String> readyTopics = this.accumulator.ready(now);
        // create produce requests
        Map<String, List<PubsubBatch>> batches = this.accumulator.drain(readyTopics, this.maxRequestSize, now);
        if (guaranteeMessageOrder) {
            // Mute all the partitions drained
            for (List<PubsubBatch> batchList : batches.values()) {
                for (PubsubBatch batch : batchList) {
                    synchronized (accumulator) {
                        if (accumulator.isMutedTopic(batch.topic)) {
                            log.info("Another thread got same ordered topic before lock, removing.", batch.topic);
                        } else {
                            this.accumulator.muteTopic(batch.topic);
                        }
                    }
                }
            }
        }

        List<PubsubBatch> expiredBatches = this.accumulator.abortExpiredBatches(this.requestTimeout, now);
        // update sensors
        for (PubsubBatch expiredBatch : expiredBatches)
            this.sensors.recordErrors(expiredBatch.topic, expiredBatch.recordCount);

        sensors.updateProduceRequestMetrics(batches);

        if (!readyTopics.isEmpty()) {
            log.trace("Topics with data ready to send: {}", readyTopics);
        }
        sendProduceRequests(batches, now);
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     */
    public void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        this.accumulator.close();
        this.running = false;
        this.executor.shutdown();
    }

    /**
     * Closes the sender without sending out any pending messages.
     */
    public void forceClose() {
        this.forceClose = true;
        initiateClose();
    }

    /**
     * Complete or retry the given batch of records.
     *
     * @param batch The record batch
     * @param error The error (or null if none)
     * @param baseOffset The base offset assigned to the records if successful
     * @param timestamp The timestamp returned by the broker for this batch
     */
    private void completeBatch(PubsubBatch batch, Errors error, long baseOffset, long timestamp) {
        if (error != Errors.NONE && canRetry(batch, error)) {
            // retry
            log.warn("Got error response on topic {}, retrying ({} attempts left). Error: {}",
                    batch.topic,
                    this.retries - batch.attempts - 1,
                    error);
            batch.done(baseOffset, timestamp, error.exception());
            this.accumulator.reenqueue(batch, time.milliseconds());
            this.sensors.recordRetries(batch.topic, batch.recordCount);
        } else {
            RuntimeException exception;
            if (error == Errors.TOPIC_AUTHORIZATION_FAILED)
                exception = new TopicAuthorizationException(batch.topic);
            else
                exception = error.exception();
            // tell the user the result of their request
            batch.done(baseOffset, timestamp, exception);
            this.accumulator.deallocate(batch);
            if (error != Errors.NONE)
                this.sensors.recordErrors(batch.topic, batch.recordCount);
        }

        // Unmute the completed partition.
        if (guaranteeMessageOrder)
            this.accumulator.unmuteTopic(batch.topic);
    }

    /**
     * We can retry a send if the error is transient and the number of attempts taken is fewer than the maximum allowed
     */
    private boolean canRetry(PubsubBatch batch, Errors error) {
        return batch.attempts < this.retries && error.exception() instanceof RetriableException;
    }

    /**
     * Transfer the record batches into a list of produce requests on a per-node basis
     */
    private void sendProduceRequests(Map<String, List<PubsubBatch>> collated, long now) {
        for (Map.Entry<String, List<PubsubBatch>> entry : collated.entrySet())
            sendProduceRequest(now, requestTimeout, entry.getValue());
    }

    /**
     * Create a produce request from the given record batches
     */
    private void sendProduceRequest(long sendTime, long timeout, List<PubsubBatch> batches) {
        for (PubsubBatch batch : batches) {
            PublishRequest.Builder request = PublishRequest.newBuilder().setTopic(batch.topic);
            request.addMessages(PubsubMessage.newBuilder()
                    .setData(ByteString.copyFrom(batch.records.buffer())));
            executor.submit(new ProduceRequestThread(sendTime, timeout, batch, request.build()));
            log.trace("Sent produce request to topic {}", batch.topic);
        }
    }

    private class ProduceRequestThread implements Runnable {
        private PublishRequest request;
        private PubsubBatch batch;
        private long timeout;
        private long sendTime;

        public ProduceRequestThread(long sendTime, long timeout, PubsubBatch batch, PublishRequest request) {
            this.timeout = timeout;
            this.sendTime = sendTime;
            this.request = request;
            this.batch = batch;
        }

        @Override
        public void run() {
            long receivedTime = time.milliseconds();
            ListenableFuture<PublishResponse> future = stub.publish(request);
            while (true) {
                try {
                    PublishResponse response = future.get(timeout, TimeUnit.MILLISECONDS);
                    log.trace("Receved produce response from topic {}", batch.topic);
                    String id = response.getMessageIds(0);
                    long offset = Long.valueOf(id);
                    completeBatch(batch, Errors.NONE, offset, receivedTime);
                    return;
                } catch (InterruptedException e) {
                    log.warn("Accessing publish future was interrupted, retrying");
                } catch (TimeoutException e) {
                    completeBatch(batch, Errors.REQUEST_TIMED_OUT, -1L, receivedTime);
                    return;
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof StatusRuntimeException) {
                        Status.Code code = ((StatusRuntimeException) cause).getStatus().getCode();
                        switch (code) {
                            case ABORTED:
                            case CANCELLED:
                            case INTERNAL:
                                completeBatch(batch, Errors.NETWORK_EXCEPTION, -1L, receivedTime);
                                break;
                            case DATA_LOSS:
                                completeBatch(batch, Errors.CORRUPT_MESSAGE, -1L, receivedTime);
                                break;
                            case DEADLINE_EXCEEDED:
                                completeBatch(batch, Errors.REQUEST_TIMED_OUT, -1L, receivedTime);
                                break;
                            case ALREADY_EXISTS:
                            case OUT_OF_RANGE:
                            case INVALID_ARGUMENT:
                                completeBatch(batch, Errors.INVALID_REQUEST, -1L, receivedTime);
                                break;
                            case NOT_FOUND:
                                completeBatch(batch, Errors.INVALID_TOPIC_EXCEPTION, -1L, receivedTime);
                                break;
                            case RESOURCE_EXHAUSTED:
                                completeBatch(batch, Errors.RECORD_LIST_TOO_LARGE, -1L, receivedTime);
                                break;
                            case PERMISSION_DENIED:
                            case UNAUTHENTICATED:
                                completeBatch(batch, Errors.TOPIC_AUTHORIZATION_FAILED, -1L, receivedTime);
                                break;
                            case FAILED_PRECONDITION:
                            case UNAVAILABLE:
                                completeBatch(batch, Errors.BROKER_NOT_AVAILABLE, -1L, receivedTime);
                                break;
                            case UNIMPLEMENTED:
                                completeBatch(batch, Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, -1L, receivedTime);
                                break;
                            default:
                                completeBatch(batch, Errors.UNKNOWN, -1L, receivedTime);
                                break;
                        }
                    } else { // Status is not StatusRuntimeException
                        completeBatch(batch, Errors.UNKNOWN, -1L, receivedTime);
                    }
                    return;
                }
                sensors.recordLatency(batch.topic, receivedTime - sendTime);
            }
        }
    }

    private class PubsubSenderMetrics {
        private final Metrics metrics;
        public final Sensor retrySensor;
        public final Sensor errorSensor;
        public final Sensor queueTimeSensor;
        public final Sensor requestTimeSensor;
        public final Sensor recordsPerRequestSensor;
        public final Sensor batchSizeSensor;
        public final Sensor compressionRateSensor;
        public final Sensor maxRecordSizeSensor;
        public final Sensor produceThrottleTimeSensor;

        public PubsubSenderMetrics(Metrics metrics) {
            this.metrics = metrics;
            String metricGrpName = "producer-metrics";

            this.batchSizeSensor = metrics.sensor("batch-size");
            MetricName m = metrics.metricName("batch-size-avg", metricGrpName, "The average number of bytes sent per partition per-request.");
            this.batchSizeSensor.add(m, new Avg());
            m = metrics.metricName("batch-size-max", metricGrpName, "The max number of bytes sent per partition per-request.");
            this.batchSizeSensor.add(m, new Max());

            this.compressionRateSensor = metrics.sensor("compression-rate");
            m = metrics.metricName("compression-rate-avg", metricGrpName, "The average compression rate of record batches.");
            this.compressionRateSensor.add(m, new Avg());

            this.queueTimeSensor = metrics.sensor("queue-time");
            m = metrics.metricName("record-queue-time-avg", metricGrpName, "The average time in ms record batches spent in the record accumulator.");
            this.queueTimeSensor.add(m, new Avg());
            m = metrics.metricName("record-queue-time-max", metricGrpName, "The maximum time in ms record batches spent in the record accumulator.");
            this.queueTimeSensor.add(m, new Max());

            this.requestTimeSensor = metrics.sensor("request-time");
            m = metrics.metricName("request-latency-avg", metricGrpName, "The average request latency in ms");
            this.requestTimeSensor.add(m, new Avg());
            m = metrics.metricName("request-latency-max", metricGrpName, "The maximum request latency in ms");
            this.requestTimeSensor.add(m, new Max());

            this.produceThrottleTimeSensor = metrics.sensor("produce-throttle-time");
            m = metrics.metricName("produce-throttle-time-avg", metricGrpName, "The average throttle time in ms");
            this.produceThrottleTimeSensor.add(m, new Avg());
            m = metrics.metricName("produce-throttle-time-max", metricGrpName, "The maximum throttle time in ms");
            this.produceThrottleTimeSensor.add(m, new Max());

            this.recordsPerRequestSensor = metrics.sensor("records-per-request");
            m = metrics.metricName("record-send-rate", metricGrpName, "The average number of records sent per second.");
            this.recordsPerRequestSensor.add(m, new Rate());
            m = metrics.metricName("records-per-request-avg", metricGrpName, "The average number of records per request.");
            this.recordsPerRequestSensor.add(m, new Avg());

            this.retrySensor = metrics.sensor("record-retries");
            m = metrics.metricName("record-retry-rate", metricGrpName, "The average per-second number of retried record sends");
            this.retrySensor.add(m, new Rate());

            this.errorSensor = metrics.sensor("errors");
            m = metrics.metricName("record-error-rate", metricGrpName, "The average per-second number of record sends that resulted in errors");
            this.errorSensor.add(m, new Rate());

            this.maxRecordSizeSensor = metrics.sensor("record-size-max");
            m = metrics.metricName("record-size-max", metricGrpName, "The maximum record size");
            this.maxRecordSizeSensor.add(m, new Max());
            m = metrics.metricName("record-size-avg", metricGrpName, "The average record size");
            this.maxRecordSizeSensor.add(m, new Avg());

            m = metrics.metricName("requests-in-flight", metricGrpName, "The current number of in-flight requests awaiting a response.");
            this.metrics.addMetric(m, new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return executor.getActiveCount();
                }
            });
        }

        private void maybeRegisterTopicMetrics(String topic) {
            // if one sensor of the metrics has been registered for the topic,
            // then all other sensors should have been registered; and vice versa
            String topicRecordsCountName = "topic." + topic + ".records-per-batch";
            Sensor topicRecordCount = this.metrics.getSensor(topicRecordsCountName);
            if (topicRecordCount == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic);
                String metricGrpName = "producer-topic-metrics";

                topicRecordCount = this.metrics.sensor(topicRecordsCountName);
                MetricName m = this.metrics.metricName("record-send-rate", metricGrpName, metricTags);
                topicRecordCount.add(m, new Rate());

                String topicByteRateName = "topic." + topic + ".bytes";
                Sensor topicByteRate = this.metrics.sensor(topicByteRateName);
                m = this.metrics.metricName("byte-rate", metricGrpName, metricTags);
                topicByteRate.add(m, new Rate());

                String topicCompressionRateName = "topic." + topic + ".compression-rate";
                Sensor topicCompressionRate = this.metrics.sensor(topicCompressionRateName);
                m = this.metrics.metricName("compression-rate", metricGrpName, metricTags);
                topicCompressionRate.add(m, new Avg());

                String topicRetryName = "topic." + topic + ".record-retries";
                Sensor topicRetrySensor = this.metrics.sensor(topicRetryName);
                m = this.metrics.metricName("record-retry-rate", metricGrpName, metricTags);
                topicRetrySensor.add(m, new Rate());

                String topicErrorName = "topic." + topic + ".record-errors";
                Sensor topicErrorSensor = this.metrics.sensor(topicErrorName);
                m = this.metrics.metricName("record-error-rate", metricGrpName, metricTags);
                topicErrorSensor.add(m, new Rate());
            }
        }

        public void updateProduceRequestMetrics(Map<String, List<PubsubBatch>> batches) {
            long now = time.milliseconds();
            for (List<PubsubBatch> topicBatch : batches.values()) {
                int records = 0;
                for (PubsubBatch batch : topicBatch) {
                    // register all per-topic metrics at once
                    String topic = batch.topic;
                    maybeRegisterTopicMetrics(topic);

                    // per-topic record send rate
                    String topicRecordsCountName = "topic." + topic + ".records-per-batch";
                    Sensor topicRecordCount = Utils.notNull(this.metrics.getSensor(topicRecordsCountName));
                    topicRecordCount.record(batch.recordCount);

                    // per-topic bytes send rate
                    String topicByteRateName = "topic." + topic + ".bytes";
                    Sensor topicByteRate = Utils.notNull(this.metrics.getSensor(topicByteRateName));
                    topicByteRate.record(batch.records.sizeInBytes());

                    // per-topic compression rate
                    String topicCompressionRateName = "topic." + topic + ".compression-rate";
                    Sensor topicCompressionRate = Utils.notNull(this.metrics.getSensor(topicCompressionRateName));
                    topicCompressionRate.record(batch.records.compressionRate());

                    // global metrics
                    this.batchSizeSensor.record(batch.records.sizeInBytes(), now);
                    this.queueTimeSensor.record(batch.drainedMs - batch.createdMs, now);
                    this.compressionRateSensor.record(batch.records.compressionRate());
                    this.maxRecordSizeSensor.record(batch.maxRecordSize, now);
                    records += batch.recordCount;
                }
                this.recordsPerRequestSensor.record(records, now);
            }
        }

        public void recordRetries(String topic, int count) {
            long now = time.milliseconds();
            this.retrySensor.record(count, now);
            String topicRetryName = "topic." + topic + ".record-retries";
            Sensor topicRetrySensor = this.metrics.getSensor(topicRetryName);
            if (topicRetrySensor != null)
                topicRetrySensor.record(count, now);
        }

        public void recordErrors(String topic, int count) {
            long now = time.milliseconds();
            this.errorSensor.record(count, now);
            String topicErrorName = "topic." + topic + ".record-errors";
            Sensor topicErrorSensor = this.metrics.getSensor(topicErrorName);
            if (topicErrorSensor != null)
                topicErrorSensor.record(count, now);
        }

        public void recordLatency(String node, long latency) {
            long now = time.milliseconds();
            this.requestTimeSensor.record(latency, now);
            if (!node.isEmpty()) {
                String nodeTimeName = "node-" + node + ".latency";
                Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
                if (nodeRequestTime != null)
                    nodeRequestTime.record(latency, now);
            }
        }
    }
}
