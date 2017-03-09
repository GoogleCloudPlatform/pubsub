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

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.flic.common.LoadtestProto.MessageIdentifier;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Each task is responsible for implementing its action and for creating {@link LoadTestRunner}.
 */
public abstract class Task implements Runnable {
  private final MetricsHandler metricsHandler;
  private AtomicInteger numMessages = new AtomicInteger(0);
  private final Map<MessageIdentifier, Long> identifiers = new HashMap<>();
  private final Map<MessageIdentifier, Long> identifiersToRecord = new HashMap<>();
  private final AtomicLong lastUpdateMillis = new AtomicLong(System.currentTimeMillis());
  private final long burnInTimeMillis;
  private final RateLimiter rateLimiter;
  private final Semaphore outstandingRequestLimiter;

  protected Task(StartRequest request, String type, MetricsHandler.MetricName metricName) {
    this.metricsHandler = new MetricsHandler(request.getProject(), type, metricName);
    this.burnInTimeMillis =
        Timestamps.toMillis(Timestamps.add(request.getStartTime(), request.getBurnInDuration()));
    rateLimiter = RateLimiter.create(request.getRequestRate());
    outstandingRequestLimiter = new Semaphore(request.getMaxOutstandingRequests(), false);
  }

  /** Stores the results of a single doRun call. */
  public static class RunResult {
    // If 'identifiers' and 'latencies' are both populated, the latencies[i] is the latency for
    // message identifiers[i]. If only 'latencies' is populated, we will not be able to do
    // deduplication or completeness checking, and will skip the deduplication logic. For example,
    // publisher tasks may choose to not populate 'identifiers'.
    public List<MessageIdentifier> identifiers = new ArrayList<>();
    public List<Long> latencies = new ArrayList<>();
    public int batchSize = 0;

    public void addMessageLatency(int clientId, int sequenceNumber, long latency) {
      identifiers.add(MessageIdentifier.newBuilder()
        .setPublisherClientId(clientId)
        .setSequenceNumber(sequenceNumber)
        .build());
      latencies.add(latency);
    }

    public static RunResult fromBatchSize(int batchSize) {
      RunResult result = new RunResult();
      result.batchSize = batchSize;
      return result;
    }

    public static RunResult fromMessages(
        List<MessageIdentifier> identifiers, List<Long> latencies) {
      RunResult result = new RunResult();
      result.identifiers = identifiers;
      result.latencies = latencies;
      return result;
    }

    public static RunResult empty() {
      return new RunResult();
    }
  }

  public abstract ListenableFuture<RunResult> doRun();

  @Override
  public void run() {
    try {
      if (!outstandingRequestLimiter.tryAcquire(250, TimeUnit.MILLISECONDS)) {
        return;
      }
    } catch (InterruptedException e) {
      return;
    }
    if (!rateLimiter.tryAcquire(250, TimeUnit.MILLISECONDS)) {
      outstandingRequestLimiter.release();
      return;
    }

    Stopwatch stopwatch = Stopwatch.createStarted();
    Futures.addCallback(doRun(),
        new FutureCallback<RunResult>() {
          @Override
          public void onSuccess(RunResult result) {
            stopwatch.stop();
            outstandingRequestLimiter.release();
            if (result.batchSize > 0) {
              recordBatchLatency(stopwatch.elapsed(TimeUnit.MILLISECONDS), result.batchSize);
              return;
            }
            if (result.identifiers.isEmpty()) {
              // Because result.identifiers is not populated, we are not checking for
              // duplicates, so just record the latencies immediately.
              result.latencies.forEach(l -> recordLatency(l));
              return;
            }
            recordAllMessageLatencies(result.identifiers, result.latencies);
          }

          @Override
          public void onFailure(Throwable t) {
            outstandingRequestLimiter.release();
          }
        });
  }

  List<Long> getBucketValues() {
    return metricsHandler.flushBucketValues();
  }

  int getNumberOfMessages() {
    return numMessages.get();
  }

  private void recordLatency(long millis) {
    recordBatchLatency(millis, 1);
  }

  protected void recordBatchLatency(long millis, int batchSize) {
    lastUpdateMillis.set(System.currentTimeMillis());
    if (System.currentTimeMillis() < burnInTimeMillis) {
      return;
    }
    metricsHandler.recordBatchLatency(millis, batchSize);
    numMessages.getAndAdd(batchSize);
  }

  long getLastUpdateMillis() {
    return lastUpdateMillis.get();
  }

  protected synchronized void recordMessageLatency(int clientId, int sequenceNumber, long latency) {
    identifiers.put(
        MessageIdentifier.newBuilder()
            .setPublisherClientId(clientId)
            .setSequenceNumber(sequenceNumber)
            .build(),
        latency);
    lastUpdateMillis.set(System.currentTimeMillis());
  }

  protected synchronized void recordAllMessageLatencies(
      List<MessageIdentifier> identifiers, List<Long> latencies) {
    Preconditions.checkArgument(
        identifiers.size() == latencies.size(),
        "Identifiers and latencies must be the same size (%s != %s).",
        identifiers.size(),
        latencies.size());
    for (int i = 0; i < identifiers.size(); i++) {
      this.identifiers.put(identifiers.get(i), latencies.get(i));
    }
    lastUpdateMillis.set(System.currentTimeMillis());
  }

  synchronized List<MessageIdentifier> flushMessageIdentifiers(List<MessageIdentifier> duplicates) {
    identifiersToRecord.forEach((identifier, latency) -> {
      if (!duplicates.contains(identifier)) {
        recordLatency(latency);
      }
    });
    identifiersToRecord.clear();
    identifiersToRecord.putAll(identifiers);
    identifiers.clear();
    return new ArrayList<>(identifiersToRecord.keySet());
  }

  protected void shutdown() {}
}
