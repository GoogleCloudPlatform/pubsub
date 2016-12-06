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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link LatencyDistribution}.
 */
public class LatencyDistributionTest {

  private static final double EPSILON = 0.001;
  private LatencyDistribution distribution;

  @Before
  public void setup() {
    distribution = new LatencyDistribution();
  }

  @Test
  public void testEmpty() {
    assertEquals(distribution.getMean(), 0, EPSILON);
    assertEquals(distribution.getCount(), 0, EPSILON);
    assertEquals(distribution.getSumOfSquareDeviations(), 0, EPSILON);
    assertArrayEquals(distribution.getBucketValues(),
        new long[LatencyDistribution.LATENCY_BUCKETS.length]);
  }

  @Test
  public void testOne() {
    distribution.recordLatency(1);
    assertEquals(distribution.getMean(), 1, EPSILON);
    assertEquals(distribution.getCount(), 1, EPSILON);
    assertEquals(distribution.getSumOfSquareDeviations(), 0, EPSILON);
    long[] expected = new long[LatencyDistribution.LATENCY_BUCKETS.length];
    expected[2]++;
    assertArrayEquals(distribution.getBucketValues(), expected);
  }

  @Test
  public void testBatchRecord() {
    long[] latencies = {2, 7, 31, 67, 137};
    LatencyDistribution control = new LatencyDistribution();
    for (long lat : latencies) {
      int n = 29;
      distribution.recordBatchLatency(lat, n);
      for (int i = 0; i < n; i++) {
        control.recordLatency(lat);
      }
    }
    assertEquals(distribution.getMean(), control.getMean(), EPSILON);
    assertEquals(distribution.getCount(), control.getCount(), EPSILON);
    assertEquals(distribution.getSumOfSquareDeviations(),
        control.getSumOfSquareDeviations(), EPSILON);
  }

  @Test
  public void testMany() {
    for (int i = 0; i < 10; i++) {
      distribution.recordLatency((long) Math.pow(2, i));
    }
    assertEquals(distribution.getMean(), 102.3, EPSILON);
    assertEquals(distribution.getCount(), 10, EPSILON);
  }

  @Test
  public void testCopy() {
    distribution.recordLatency(1);
    LatencyDistribution distributionCopy = distribution.copy();
    assertEquals(distributionCopy.getMean(), 1, EPSILON);
    assertEquals(distributionCopy.getCount(), 1, EPSILON);
    assertEquals(distributionCopy.getSumOfSquareDeviations(), 0, EPSILON);
    long[] expected = new long[LatencyDistribution.LATENCY_BUCKETS.length];
    expected[2]++;
    assertArrayEquals(distributionCopy.getBucketValues(), expected);
  }

  @Test
  public void testReset() {
    distribution.recordLatency(1);
    distribution.recordLatency(2);
    distribution.recordLatency(3);
    distribution.reset();
    assertEquals(distribution.getMean(), 0, EPSILON);
    assertEquals(distribution.getCount(), 0, EPSILON);
    assertEquals(distribution.getSumOfSquareDeviations(), 0, EPSILON);
    assertArrayEquals(distribution.getBucketValues(),
        new long[LatencyDistribution.LATENCY_BUCKETS.length]);
  }
}
