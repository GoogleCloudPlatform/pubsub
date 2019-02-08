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
package com.google.pubsub.clients.kafka;

import com.beust.jcommander.JCommander;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.clients.common.JavaLoadtestWorker;
import com.google.pubsub.clients.common.LoadtestTask;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.PooledWorkerTask;
import com.google.pubsub.flic.common.LoadtestProto;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Runs a task that consumes messages utilizing Kafka's implementation of the Consumer<K,V>
 * interface.
 */
class KafkaSubscriberTask extends PooledWorkerTask {
    private final long pollLength;
    private final KafkaConsumer<String, String> subscriber;

    private KafkaSubscriberTask(StartRequest request, MetricsHandler handler, int numWorkers) {
        super(request, handler, numWorkers);
        this.pollLength = Durations.toMillis(request.getKafkaOptions().getPollDuration());
        Properties props = new Properties();
        props.putAll(ImmutableMap.of(
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "group.id", "SUBSCRIBER_ID",
                "enable.auto.commit", "true",
                "session.timeout.ms", "30000"
        ));
        props.put("bootstrap.servers", request.getKafkaOptions().getBroker());
        subscriber = new KafkaConsumer<>(props);
        subscriber.subscribe(Collections.singletonList(request.getTopic()));
    }

    @Override
    public void startAction() {
        while (!isShutdown.get()) {
            ConsumerRecords<String, String> records = subscriber.poll(pollLength);
            for (ConsumerRecord<String, String> record : records) {
                LoadtestProto.KafkaMessage message = parsePayload(record.value());
                long delayMillis = Timestamps.toMillis(message.getPublishTime()) - System.currentTimeMillis();
                metricsHandler.add(message.getId(), Duration.ofMillis(delayMillis));
            }
        }
    }

    private static LoadtestProto.KafkaMessage parsePayload(String payload) {
        try {
            return LoadtestProto.KafkaMessage.parseFrom(payload.getBytes());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Invalid proto received from kafka: ", e);
        }
    }

    @Override
    protected void cleanup() {
        subscriber.close();
    }

    private static class KafkaSubscriberFactory implements Factory {
        @Override
        public LoadtestTask newTask(StartRequest request, MetricsHandler handler, int numWorkers) {
            return new KafkaSubscriberTask(request, handler, numWorkers);
        }
    }

    public static void main(String[] args) throws Exception {
        JavaLoadtestWorker.Options options = new JavaLoadtestWorker.Options();
        new JCommander(options, args);
        new JavaLoadtestWorker(options, new KafkaSubscriberFactory());
    }
}
