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

import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;

public class StatsUtils {
  /** Returns the average QPS. */
  public static double getQPS(long messageCount, Duration loadtestDuration) {
    return messageCount / (double) Durations.toSeconds(loadtestDuration);
  }

  /** Returns the average throughput in MB/s. */
  public static double getThroughput(
      long messageCount, Duration loadtestDuration, long messageSize) {
    return getQPS(messageCount, loadtestDuration) * messageSize / 1000000.0;
  }
}
