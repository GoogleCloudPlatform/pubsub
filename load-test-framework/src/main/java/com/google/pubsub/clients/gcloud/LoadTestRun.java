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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A single load-test run - that is, an object that encapsulates a single copy of the set of
 * discrete actions that constitute one load test "query" for purposes of things like the load
 * test overall QPS.
 */
public class LoadTestRun implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(LoadTestRun.class);

  private static final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor((runnable) -> {
        Thread thread = Executors.defaultThreadFactory().newThread(runnable);
        thread.setDaemon(true);
        return thread;
      });

  private final PubSub pubSub;
  private final RunParams params;
  private final String logPrefix;
  private final String payload;

  public LoadTestRun(
      PubSub pubSub,
      RunParams params,
      int index,
      String payload) {
    this.pubSub = Preconditions.checkNotNull(pubSub);
    this.params = Preconditions.checkNotNull(params);
    this.payload = payload;

    logPrefix = "Test " + index + " - (topic=" + params.topicName + ", sub=" + params.subscriptionName + "): ";
  }

  @Override
  public void run() {
    try {
      switch (params.runType) {
        case PUBLISH_RUN:
          log.info(logPrefix + "Publish started");
          publish(params.labels, params.topicName);
          break;
        case PULL_SUBSCRIPTION_RUN:
          log.info(logPrefix + "Pull started");
          final List<String> ackTokens = pull(params.subscriptionName);
          if (ackTokens != null && !ackTokens.isEmpty()) {
            if (LoadTestFlags.numModifyAckDeadline > 0) {

              Runnable modifyAckRun =
                  new Runnable() {
                    private int attemptNum = 0;

                    @Override
                    public void run() {
                      try {
                        if (attemptNum < LoadTestFlags.numModifyAckDeadline) {
                          log.info(
                              logPrefix + "ModifyAckDeadline started (attempt: " + attemptNum + 1 + "/" +
                                  LoadTestFlags.numModifyAckDeadline + ")");
                          modifyAckDeadline(params.subscriptionName, ackTokens);
                          scheduler.schedule(
                              this,
                              LoadTestFlags.modifyAckWaitTime.toMillis(),
                              TimeUnit.MILLISECONDS);
                          attemptNum++;
                        } else {
                          log.info(
                              logPrefix + "ModifyAckDeadline ended. Acknowledge started");
                          acknowledge(params.subscriptionName, ackTokens);
                        }
                      } catch (RetryableException e) {
                        log.warn(logPrefix + "Operation failed", e.getCause());
                      }
                    }
                  };
              scheduler.schedule(
                  modifyAckRun,
                  LoadTestFlags.modifyAckWaitTime.toMillis(),
                  TimeUnit.MILLISECONDS);
            } else {
              log.info(logPrefix + "Acknowledge started");
              acknowledge(params.subscriptionName, ackTokens);
            }
          }
          break;
      }
    } catch (RetryableException e) {
      log.warn(logPrefix + "Operation failed", e.getCause());
    }
  }

  public void publish(
      Map<String, String> labels,
      String topic) throws RetryableException {
    String result = "unknown";
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    try {
      List<Message> messages = new ArrayList<>(LoadTestFlags.publishBatchSize);
      for (int i = 0; i < LoadTestFlags.publishBatchSize; i++) {
        messages.add(Message.builder(payload).attributes(labels).build());
      }
      stopwatch.start();
      pubSub.publish(topic, messages);
      stopwatch.stop();
      result = "succeeded";
    } catch (PubSubException e) {
      stopwatch.stop();
      result = e.toString();
      log.warn(logPrefix + "Publish request failed", e);
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

  public List<String> pull(String subscription) throws RetryableException {
    String result = "unknown";
    Stopwatch stopwatch = Stopwatch.createUnstarted();
    try {
      List<String> ackIds = new ArrayList<>();
      stopwatch.start();
      Iterator<ReceivedMessage> responses = pubSub.pull(subscription, LoadTestFlags.pullBatchSize);
      stopwatch.stop();
      responses.forEachRemaining((response) -> ackIds.add(response.ackId()));
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

  public void modifyAckDeadline(String subscription, List<String> ackTokens)
      throws RetryableException {
    String result = "unknown";
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      pubSub.modifyAckDeadline(subscription, LoadTestFlags.modifyAckDeadlineSeconds, TimeUnit.SECONDS, ackTokens);
      stopwatch.stop();
      result = "succeeded";
    } catch (PubSubException e) {
      stopwatch.stop();
      log.warn(logPrefix + "ModifyAckDeadline failed", e);
      result = e.toString();
      throw new RetryableException(e);
    } catch (Exception e) {
      log.warn(logPrefix + "ModifyAckDeadline failed with unknown exception", e);
      throw e;
    } finally {
      long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      log.info(logPrefix + "ModifyAckDeadline attempt (" + result + ") took " + elapsed + " ms");
    }
  }

  public void acknowledge(String subscription, List<String> ackTokens) throws RetryableException {
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
  public class RetryableException extends Exception {
    public RetryableException(Throwable cause) {
      super(cause);
    }
  }
}
