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

import com.google.cloud.pubsub.Message;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubException;
import com.google.cloud.pubsub.ReceivedMessage;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.pubsub.clients.common.MetricsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A single load-test run - that is, an object that encapsulates a single copy of the set of
 * discrete actions that constitute one load test "query" for purposes of things like the load
 * test overall QPS.
 */
class LoadTestRun implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(LoadTestRun.class);
  static String topic = "load-test-topic";
  static String subscription = "load-test-subscription"; // set null for Publish
  static int batchSize = 100;
  static MetricsHandler metricsHandler;
  private final PubSub pubSub;
  private final String logPrefix;
  private final String payload;

  LoadTestRun(PubSub pubSub, String payload) {
    this.pubSub = Preconditions.checkNotNull(pubSub);
    this.payload = payload;
    logPrefix = "Test - (topic=" + topic + ", sub=" + subscription + "): ";
  }

  @Override
  public void run() {
    try {
      if (subscription.isEmpty()) {
        log.info(logPrefix + "Publish started");
        publish(topic);
        return;
      }
      log.info(logPrefix + "Pull started");
      final List<String> ackTokens = pull(subscription);
      if (ackTokens != null && !ackTokens.isEmpty()) {
        log.info(logPrefix + "Acknowledge started");
        acknowledge(subscription, ackTokens);
      }
    } catch (RetryableException e) {
      log.warn(logPrefix + "Operation failed", e.getCause());
    }
  }

  private void publish(
      String topic) throws RetryableException {
    String result = "unknown";
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    try {
      List<Message> messages = new ArrayList<>(batchSize);
      String sendTime = String.valueOf(System.currentTimeMillis());
      for (int i = 0; i < batchSize; i++) {
        messages.add(Message.builder(payload).addAttribute("sendTime", sendTime).build());
      }
      stopwatch.start();
      pubSub.publish(topic, messages);
      stopwatch.stop();
      metricsHandler.recordPublishLatency(stopwatch.elapsed(TimeUnit.MILLISECONDS));
      result = "succeeded";
    } catch (PubSubException e) {
      stopwatch.stop();
      result = e.toString();
      log.error(logPrefix + "Publish request failed", e);
      throw new RetryableException(e);
    } catch (Exception e) {
      log.warn(logPrefix + "Publish request failed with unknown exception", e);
      throw e;
    } finally {
      log.info(
          logPrefix + "Publish request took " + stopwatch.elapsed(TimeUnit.MILLISECONDS) +
              " ms (result=" + result + ")");
    }
  }

  private List<String> pull(String subscription) throws RetryableException {
    String result = "unknown";
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    try {
      List<String> ackIds = new ArrayList<>();
      stopwatch.start();
      Iterator<ReceivedMessage> responses = pubSub.pull(subscription, batchSize);
      stopwatch.stop();
      long now = System.currentTimeMillis();
      responses.forEachRemaining((response) -> {
        ackIds.add(response.ackId());
        metricsHandler.recordEndToEndLatency(now - Long.parseLong(response.attributes().get("sendTime")));
      });
      if (ackIds.isEmpty()) {
        result = "no-messages";
        log.info(logPrefix + "Pull returned no messages");
      } else {
        result = "succeeded";
        log.info(logPrefix + "Successfully pulled " + ackIds.size() + " messages");
      }
      return ackIds;
    } catch (PubSubException e) {
      stopwatch.stop();
      log.warn(logPrefix + "Pull failed", e);
      result = e.toString();
      throw new RetryableException(e);
    } catch (Exception e) {
      log.warn(logPrefix + "Pull failed with unknown exception", e);
      throw e;
    } finally {
      long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      log.info(logPrefix + "Pull request for subscription " + subscription +
          " took " + elapsed + " ms (" + result + ")");
    }
  }

  private void acknowledge(String subscription, List<String> ackTokens) throws RetryableException {
    String result = "unknown";
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      pubSub.ack(subscription, ackTokens);
      stopwatch.stop();
      result = "succeeded";
    } catch (PubSubException e) {
      stopwatch.stop();
      log.warn(logPrefix + "Acknowledge failed", e);
      result = e.toString();
      throw new RetryableException(e);
    } catch (Exception e) {
      log.warn(logPrefix + "Ack request failed with unknown exception", e);
      throw e;
    } finally {
      long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      log.info(logPrefix + "Acknowledge attempt (" + result + ") took " + elapsed + " ms");
    }
  }

  /**
   * Type for exceptions that may be retried during the load test. The cause is the true exception.
   */
  private class RetryableException extends Exception {
    RetryableException(Throwable cause) {
      super(cause);
    }
  }
}
