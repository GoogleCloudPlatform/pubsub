package com.google.pubsub.flic.common;

import com.google.common.base.Preconditions;

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
      500.0,
      1000.0,
      2000.0,
      3000.0,
      10000.0,
      20000.0,
      100000.0,
      400000.0,
      1000000.0,
      10000000.0,
      100000000.0,
      1000000000.0,
      (double) Integer.MAX_VALUE
  };
  private final long[] bucketValues = new long[LATENCY_BUCKETS.length];
  private long count = 0;
  private double mean = 0;
  private double sumOfSquaredDeviation = 0;

  public LatencyDistribution() {
  }

  public static String getNthPercentile(long[] bucketValues, double percentile) {
    Preconditions.checkArgument(percentile > 0.0);
    Preconditions.checkArgument(percentile < 1.0);
    long total = 0;
    for (int i = 0; i < LATENCY_BUCKETS.length; i++) {
      total += bucketValues[i];
    }
    if (total == 0) {
      return "N/A";
    }
    long count = (long) (total * percentile);
    for (int i = LATENCY_BUCKETS.length - 1; i > 0; i--) {
      total -= bucketValues[i];
      if (total <= count) {
        return LATENCY_BUCKETS[i - 1] + " - " + LATENCY_BUCKETS[i];
      }
    }
    return "N/A";
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

  public long[] getBucketValues() {
    return bucketValues;
  }

  public void recordLatency(long latencyMs) {
    synchronized (this) {
      count++;
      double dev = latencyMs - mean;
      mean += dev / count;
      sumOfSquaredDeviation += dev * (latencyMs - mean);
    }

    boolean bucketFound = false;
    for (int i = 0; i < LATENCY_BUCKETS.length; i++) {
      double bucket = LATENCY_BUCKETS[i];
      if (latencyMs < bucket) {
        synchronized (this) {
          bucketValues[i]++;
        }
        bucketFound = true;
        break;
      }
    }
    if (!bucketFound) {
      synchronized (this) {
        bucketValues[LATENCY_BUCKETS.length - 1]++;
      }
    }
  }
}
