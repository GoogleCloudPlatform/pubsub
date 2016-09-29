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

import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A service that adjusts the rate of the load test by periodically passing new rate information to
 * a {@link LoadTestLauncher}.
 */
public class LoadTestPacer extends AbstractScheduledService {

  private static final Logger log = LoggerFactory.getLogger(LoadTestPacer.class);

  @Parameter(names = {"--adjustment_interval_seconds"},
      description = "Adjust the rate of load tests to match the intended rate at this interval.")
  private static final Integer adjustmentIntervalSeconds = 10;

  private final LoadTestLauncher launcher;
  private final double qpsDeltaPerSecond;

  private final double targetQps;

  private Instant lastAdjustmentTimestamp;

  /**
   * Creates a pacer that adjusts the rate on the launcher until it reaches the desired target rate.
   *
   * @param launcher          Launcher instance.
   * @param targetQps         The target rate to reach for the launcher.
   * @param qpsDeltaPerSecond The desired change in the load test rate per second.
   */
  public LoadTestPacer(LoadTestLauncher launcher, double targetQps, double qpsDeltaPerSecond) {
    this.launcher = Preconditions.checkNotNull(launcher);

    Preconditions.checkArgument(targetQps > 0);
    Preconditions.checkArgument(qpsDeltaPerSecond > 0);

    this.targetQps = targetQps;
    this.qpsDeltaPerSecond = qpsDeltaPerSecond;

    lastAdjustmentTimestamp = Instant.now();
  }

  @Override
  protected void runOneIteration() throws Exception {
    Instant currentAdjustmentTimestamp = Instant.now();

    double secondsSinceLastAdjustment =
        (currentAdjustmentTimestamp.getMillis() - lastAdjustmentTimestamp.getMillis()) / 1000.0;

    double maxAdjustment = secondsSinceLastAdjustment * qpsDeltaPerSecond;

    double newQps;
    if (Math.abs(launcher.getRate() - targetQps) < maxAdjustment) {
      newQps = targetQps;
    } else if (launcher.getRate() > targetQps) {
      newQps = launcher.getRate() - maxAdjustment;
    } else {
      newQps = launcher.getRate() + maxAdjustment;
    }

    if (launcher.getRate() == newQps) {
      return;
    }

    log.info("Adjusting QPS from " + launcher.getRate() + " to " + newQps);
    launcher.setRate(newQps);

    lastAdjustmentTimestamp = currentAdjustmentTimestamp;
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        adjustmentIntervalSeconds, adjustmentIntervalSeconds, TimeUnit.SECONDS);
  }
}
