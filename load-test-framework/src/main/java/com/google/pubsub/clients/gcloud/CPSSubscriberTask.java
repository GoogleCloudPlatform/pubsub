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
package com.google.pubsub.clients.gcloud;

import com.beust.jcommander.JCommander;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.clients.common.JavaLoadtestWorker;
import com.google.pubsub.clients.common.LoadtestTask;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.flic.common.LoadtestProto;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Runs a task that consumes messages from a Cloud Pub/Sub subscription.
 */
class CPSSubscriberTask implements LoadtestTask, MessageReceiver {
    private static final Logger log = LoggerFactory.getLogger(CPSSubscriberTask.class);
    private static final long BYTES_PER_WORKER = 100000000;  // 100MB per worker outstanding
    private final MetricsHandler metricsHandler;
    private final Subscriber subscriber;

    private CPSSubscriberTask(StartRequest request, MetricsHandler metricsHandler, int workerCount) {
        this.metricsHandler = metricsHandler;
        ProjectSubscriptionName subscription =
                ProjectSubscriptionName.of(request.getProject(), request.getPubsubOptions().getSubscription());
        try {
            this.subscriber =
                    Subscriber.newBuilder(subscription, this)
                            .setParallelPullCount(workerCount)
                            .setFlowControlSettings(FlowControlSettings.newBuilder()
                                    .setMaxOutstandingElementCount(Long.MAX_VALUE)
                                    .setMaxOutstandingRequestBytes(BYTES_PER_WORKER * workerCount)
                                    .build())
                            .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void receiveMessage(final PubsubMessage message, final AckReplyConsumer consumer) {

        this.metricsHandler.add(
                LoadtestProto.MessageIdentifier.newBuilder()
                        .setPublisherClientId(Integer.parseInt(message.getAttributesMap().get("clientId")))
                        .setSequenceNumber(Integer.parseInt(message.getAttributesMap().get("sequenceNumber")))
                        .build(),
                Duration.ofMillis(System.currentTimeMillis() - Long.parseLong(message.getAttributesMap().get("sendTime")))
        );
        consumer.ack();
    }

    @Override
    public void start() {
        try {
            subscriber.startAsync().awaitRunning();
        } catch (Exception e) {
            log.error("Fatal error from subscriber.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        subscriber.stopAsync().awaitTerminated();
    }

    private static class CPSSubscriberFactory implements Factory {
        @Override
        public LoadtestTask newTask(StartRequest request, MetricsHandler handler, int workerCount) {
            return new CPSSubscriberTask(request, handler, workerCount);
        }
    }

    public static void main(String[] args) throws Exception {
        JavaLoadtestWorker.Options options = new JavaLoadtestWorker.Options();
        new JCommander(options, args);
        new JavaLoadtestWorker(options, new CPSSubscriberFactory());
    }
}
