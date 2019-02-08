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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.clients.common.AbstractPublisher;
import com.google.pubsub.clients.common.JavaLoadtestWorker;
import com.google.pubsub.clients.common.LoadtestTask;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.flic.common.LoadtestProto;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a task that publishes messages utilizing Kafka's implementation of the Producer<K,V>
 * interface
 */
class KafkaPublisherTask extends AbstractPublisher {
    private static final Logger log = LoggerFactory.getLogger(KafkaPublisherTask.class);
    private final String topic;
    private final KafkaProducer<String, String> publisher;

    private KafkaPublisherTask(LoadtestProto.StartRequest request, MetricsHandler handler, int workerCount) {
        super(request, handler, workerCount);
        this.topic = request.getTopic();
        Properties props = new Properties();
        props.putAll(new ImmutableMap.Builder<>()
                .put("max.block.ms", "30000")
                .put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                .put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                .put("acks", "all")
                .put("bootstrap.servers", request.getKafkaOptions().getBroker())
                .put("buffer.memory", Integer.toString(1000 * 1000 * 1000)) // 1 GB
                .put("batch.size", Long.toString(getBatchSize(
                        request.getPublisherOptions().getBatchSize(),
                        request.getPublisherOptions().getMessageSize()
                )))
                .put("linger.ms", Long.toString(Durations.toMillis(request.getPublisherOptions().getBatchDuration())))
                .build()
        );
        this.publisher = new KafkaProducer<>(props);
    }

    private static long getBatchSize(long messageBatchSize, long messageSize) {
        return messageBatchSize * messageSize;
    }

    @Override
    public ListenableFuture<Void> publish(int clientId, int sequenceNumber, long publishTimestampMillis) {
        SettableFuture<Void> result = SettableFuture.create();
        publisher.send(
                new ProducerRecord<>(
                        topic, null, System.currentTimeMillis(), null,
                        makePayload(clientId, sequenceNumber, publishTimestampMillis)),
                (metadata, exception) -> {
                    if (exception != null) {
                        result.setException(exception);
                        return;
                    }
                    result.set(null);
                });
        return result;
    }

    private String makePayload(int clientId, int sequenceNumber, long publishTimestampMillis) {
        return new String(
                LoadtestProto.KafkaMessage.newBuilder()
                        .setId(LoadtestProto.MessageIdentifier.newBuilder()
                                .setPublisherClientId(clientId)
                                .setSequenceNumber(sequenceNumber))
                        .setPublishTime(Timestamps.fromMillis(publishTimestampMillis))
                        .setPayload(getPayload())
                        .build().toByteArray());
    }

    @Override
    public void cleanup() {
        publisher.close();
    }

    private static class KafkaPublisherFactory implements Factory {
        @Override
        public LoadtestTask newTask(StartRequest request, MetricsHandler handler, int numWorkers) {
            return new KafkaPublisherTask(request, handler, numWorkers);
        }
    }

    public static void main(String[] args) throws Exception {
        JavaLoadtestWorker.Options options = new JavaLoadtestWorker.Options();
        new JCommander(options, args);
        new JavaLoadtestWorker(options, new KafkaPublisherFactory());
    }
}
