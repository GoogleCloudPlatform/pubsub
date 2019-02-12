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
package com.google.pubsub.clients.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.pubsub.flic.common.LoadtestProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that is used to record metrics related to the execution of the load tests, such metrics
 * are recorded using Google's Cloud Monitoring API.
 */
public class MetricsHandler {
    private static final Logger log = LoggerFactory.getLogger(MetricsHandler.class);
    private final ShardedBlockingQueue<MessageAndLatency> messageQueue;
    private final AtomicInteger failures;
    private final boolean includeIds;

    public MetricsHandler(boolean includeIds) {
        this.includeIds = includeIds;
        this.messageQueue = new ShardedBlockingQueue<>();
        this.failures = new AtomicInteger(0);
    }

    class MessageAndLatency {
        LoadtestProto.MessageIdentifier id;
        Duration latency;
    }

    public void add(LoadtestProto.MessageIdentifier id, Duration latency) {
        MessageAndLatency ml = new MessageAndLatency();
        if (includeIds) {
            ml.id = id;
        } else {
            ml.id = null;
        }
        ml.latency = latency;
        messageQueue.add(ml);
    }

    public void addFailure() {
        failures.incrementAndGet();
    }

    public LoadtestProto.CheckResponse check() {
        LoadtestProto.CheckResponse.Builder builder = LoadtestProto.CheckResponse.newBuilder();
        builder.setFailed(failures.getAndSet(0));

        ArrayList<MessageAndLatency> values = new ArrayList<>();
        messageQueue.drainTo(values);
        values.forEach(value -> {
            if (value.id != null) {
                builder.addReceivedMessages(value.id);
            }
            double raw_bucket = Math.log(value.latency.toMillis()) / Math.log(1.5);
            int bucket = Math.max((int) Math.floor(raw_bucket), 0);
            while (builder.getBucketValuesCount() - 1 < bucket) {
                builder.addBucketValues(0);
            }
            builder.setBucketValues(bucket, builder.getBucketValues(bucket) + 1);
        });

        return builder.build();
    }
}
