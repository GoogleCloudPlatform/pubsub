/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.google.pubsub.flic.common;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;

/**
 * Takes latency measurements and stores them in buckets for more efficient storage, along with
 * utilities to calculate percentiles for analysis of results.
 */
public class LatencyTracker {
  // Histogram of latencies, each one a delta from the previous CheckResponse sent.
  // The bounds of the nth bucket (starting from the 0th bucket) are
  // [1.5^(n-1), 1.5^n) milliseconds.  The lower bound of the 0th bucket is 0 seconds.
  private final ArrayList<Long> bucketValues = new ArrayList<>();
  private long count = 0;
  private long midpointSum = 0;

  public LatencyTracker() {}

  private int getNthPercentileIndex(double percentile) {
    Preconditions.checkArgument(percentile > 0.0);
    Preconditions.checkArgument(percentile < 100.0);
    long total = bucketValues.stream().mapToLong(x -> x).sum();
    if (total == 0) {
      return 0;
    }
    long count = (long) (total * percentile / 100.0);
    for (int i = bucketValues.size() - 1; i > 0; i--) {
      total -= bucketValues.get(i);
      if (total < count) {
        return i;
      }
    }
    throw new RuntimeException("Programming error: negative values appear in buckets.");
  }

  private double bucketIndexBoundary(int index) {
    if (index < 0) {
      return 0;
    }
    return Math.pow(1.5, index);
  }

  private double bucketMidpoint(int index) {
    return (bucketIndexBoundary(index - 1) + bucketIndexBoundary(index)) / 2;
  }

  public synchronized double getNthPercentileUpperBound(double percentile) {
    return bucketIndexBoundary(getNthPercentileIndex(percentile));
  }

  public synchronized double getNthPercentileLowerBound(double percentile) {
    return bucketIndexBoundary(getNthPercentileIndex(percentile) - 1);
  }

  public synchronized String getNthPercentile(double percentile) {
    return getNthPercentileLowerBound(percentile) + " - " + getNthPercentileUpperBound(percentile);
  }

  public synchronized String getNthPercentileMidpoint(double percentile) {
    return Double.toString(
        (getNthPercentileLowerBound(percentile) + getNthPercentileUpperBound(percentile)) / 2);
  }

  public synchronized long getCount() {
    return count;
  }

  public synchronized double getStdDeviation() {
    double mean = getMean();
    double sse = 0;
    for (int i = 0; i < bucketValues.size(); i++) {
      sse += Math.pow(mean - bucketMidpoint(i), 2);
    }
    return Math.sqrt(sse);
  }

  public synchronized double getMean() {
    return ((double) midpointSum) / count;
  }

  public synchronized LatencyTracker copy() {
    LatencyTracker latencyTracker = new LatencyTracker();
    latencyTracker.count = count;
    latencyTracker.midpointSum = midpointSum;
    latencyTracker.bucketValues.addAll(bucketValues);
    return latencyTracker;
  }

  public synchronized List<Long> getBucketValuesAsList() {
    return bucketValues;
  }

  public synchronized void addLatencies(List<Long> values) {
    while (bucketValues.size() <= values.size()) {
      bucketValues.add(0L);
    }
    for (int i = 0; i < values.size(); i++) {
      bucketValues.set(i, bucketValues.get(i) + values.get(i));
      count += values.get(i);
      midpointSum += bucketMidpoint(i) * values.get(i);
    }
  }
}
