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

package com.google.pubsub.flic.common;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.stream.LongStream;
import org.apache.commons.lang3.ArrayUtils;


/**
 * Takes latency measurements and stores them in buckets for more efficient storage, along with
 * utilities to calculate percentiles for analysis of results.
 */
public class LatencyDistribution {
  public static final double[] LATENCY_BUCKETS = {
      0.0,
      1.0,
      5.0,
      10.0,
      20.0,
      40.0,
      60.0,
      80.0,
      100.0,
      150.0,
      200.0,
      250.0,
      300.0,
      400.0,
      500.0,
      600.0,
      700.0,
      800.0,
      900.0,
      1000.0,
      1500.0,
      2000.0,
      5000.0,
      10000.0,
      50000.0,
      100000.0,
      500000.0,
      1000000.0,
      10000000.0,
      100000000.0,
      1000000000.0,
      Integer.MAX_VALUE
  };
  private final long[] bucketValues = new long[LATENCY_BUCKETS.length];
  private long count = 0;
  private double mean = 0;
  private double sumOfSquaredDeviation = 0;

  public LatencyDistribution() {
  }

  private static int getNthPercentileIndex(long[] bucketValues, double percentile) {
    Preconditions.checkArgument(percentile > 0.0);
    Preconditions.checkArgument(percentile < 100.0);
    long total = LongStream.of(bucketValues).sum();
    if (total == 0) {
      return 0;
    }
    long count = (long) (total * percentile / 100.0);
    for (int i = LATENCY_BUCKETS.length - 1; i > 0; i--) {
      total -= bucketValues[i];
      if (total < count) {
        return i;
      }
    }
    return -1;
  }

  public static double getNthPercentileUpperBound(long[] bucketValues, double percentile) {
    return LATENCY_BUCKETS[Math.max(0, getNthPercentileIndex(bucketValues, percentile))];
  }

  public static String getNthPercentile(long[] bucketValues, double percentile) {
    int index = getNthPercentileIndex(bucketValues, percentile);
    if (index < 1) {
      return "N/A";
    } else if (index == 0) {
      return "0.0 - " + Double.toString(LATENCY_BUCKETS[0]);
    }
    return LATENCY_BUCKETS[index - 1] + " - " + LATENCY_BUCKETS[index];
  }

  public static String getNthPercentileMidpoint(long[] bucketValues, double percentile) {
    int index = getNthPercentileIndex(bucketValues, percentile);
    if (index < 1) {
      return "N/A";
    } else if (index == 0) {
      return Double.toString(LATENCY_BUCKETS[0] / 2);
    }
    return Double.toString((LATENCY_BUCKETS[index - 1] + LATENCY_BUCKETS[index]) / 2);
  }

  public synchronized void reset() {
    for (int i = 0; i < LATENCY_BUCKETS.length; i++) {
      bucketValues[i] = 0;
    }
    count = 0;
    mean = 0;
    sumOfSquaredDeviation = 0;
  }

  public long getCount() {
    return count;
  }

  public double getSumOfSquareDeviations() {
    return sumOfSquaredDeviation;
  }

  public double getMean() {
    return mean;
  }

  long[] getBucketValues() {
    return bucketValues;
  }

  public synchronized LatencyDistribution copy() {
    LatencyDistribution latencyDistribution = new LatencyDistribution();
    latencyDistribution.count = count;
    latencyDistribution.mean = mean;
    latencyDistribution.sumOfSquaredDeviation = sumOfSquaredDeviation;
    System.arraycopy(bucketValues, 0, latencyDistribution.bucketValues, 0, LATENCY_BUCKETS.length);
    return latencyDistribution;
  }

  public List<Long> getBucketValuesAsList() {
    return Arrays.asList(ArrayUtils.toObject(bucketValues));
  }

  public void recordLatency(long latencyMs) {
    synchronized (this) {
      count++;
      double dev = latencyMs - mean;
      mean += dev / count;
      sumOfSquaredDeviation += dev * (latencyMs - mean);
    }
    binarySearchBuckets(latencyMs, 1);
  }

  public void recordLatencyBatch(long latencyMs, int batchSize) {
    synchronized (this) {
      double dev = latencyMs - mean;
      mean = (mean * count + latencyMs * batchSize) / (count + batchSize);
      count += batchSize;
      sumOfSquaredDeviation += dev * (latencyMs - mean) * batchSize;
    }
    binarySearchBuckets(latencyMs, batchSize);
  }

  private void binarySearchBuckets(long latencyMs, int batchSize) {
    int l = 0;
    int r = LATENCY_BUCKETS.length - 1;
    int i;
    while (true) {
      i = (l + r) / 2;
      if (i <= 0) {
        break;
      } else if (i == LATENCY_BUCKETS.length - 1) {
        break;
      }
      if (latencyMs < LATENCY_BUCKETS[i]) {
        if (latencyMs >= LATENCY_BUCKETS[i - 1]) {
          break;
        }
        r = i - 2; // -2 because that index will check the bucket to its right
        continue;
      } else if (latencyMs < LATENCY_BUCKETS[++i]) { // check bucket on either side
        break;
      }
      l = i + 1;
    }
    synchronized (this) {
      bucketValues[i]+= batchSize;
    }
  }
}
