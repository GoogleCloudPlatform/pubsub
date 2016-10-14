package com.google.pubsub.flic.common;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.LongStream;

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
      (double) Integer.MAX_VALUE
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
      if (total <= count) {
        return i;
      }
    }
    return 0;
  }

  public static double getNthPercentileUpperBound(long[] bucketValues, double percentile) {
    return LATENCY_BUCKETS[getNthPercentileIndex(bucketValues, percentile)];
  }

  public static String getNthPercentile(long[] bucketValues, double percentile) {
    int index = getNthPercentileIndex(bucketValues, percentile);
    if (index == 0) {
      return "N/A";
    }
    return LATENCY_BUCKETS[index - 1] + " - " + LATENCY_BUCKETS[index];
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

    for (int i = 0; i < LATENCY_BUCKETS.length; i++) {
      double bucket = LATENCY_BUCKETS[i];
      if (latencyMs < bucket) {
        synchronized (this) {
          bucketValues[i]++;
        }
        return;
      }
    }
    synchronized (this) {
      bucketValues[LATENCY_BUCKETS.length - 1]++;
    }
  }
}
