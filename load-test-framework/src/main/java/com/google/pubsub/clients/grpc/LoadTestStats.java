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
package com.google.pubsub.clients.grpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A class that maintains in memory statistics of the load test.
 */
class LoadTestStats {
  private static final Logger log = LoggerFactory.getLogger(LoadTestStats.class);

  private final String statsType;
  private final AtomicLong successCounter = new AtomicLong();
  private final AtomicLong messagesCounter = new AtomicLong();
  private final AtomicLong errorCounter = new AtomicLong();
  private long startTime;

  LoadTestStats(String statsType) {
    this.statsType = statsType;
  }

  void startTimer() {
    startTime = System.currentTimeMillis();
  }

  void recordSuccessfulRequest(int messages, long latencyMilliseconds) {
    log.debug("Latency recorded: " + latencyMilliseconds);
    successCounter.incrementAndGet();
    messagesCounter.addAndGet(messages);
  }

  void recordFailedRequest() {
    errorCounter.incrementAndGet();
  }

  void printStats() {
    long duration = (System.currentTimeMillis() - startTime) / 1000;
    long successCount = successCounter.get();
    long failCount = errorCounter.get();
    long messages = messagesCounter.get();
    long avgQps = (successCount + failCount) / duration;
    long avgMessagesPerSecond = messages / duration;
    log.info(
        "Total messages (" + statsType + "): " + messages + ", avg. QPS " + avgQps + " (successful="
            + successCount + ", errors=" + failCount + ", fail-rate=" + (float) failCount / (successCount + failCount)
            + ") Messages per second " + avgMessagesPerSecond);
  }
}
