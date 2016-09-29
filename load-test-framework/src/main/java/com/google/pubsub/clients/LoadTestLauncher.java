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
package com.google.pubsub.clients;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.Semaphore;

/**
 * A {@link com.google.common.util.concurrent.Service} that runs indefinitely on a thread.
 * It will launch LoadTestRun's on a separate thread pool at the given rate, up to the given maximum
 * number of simultaneous load test runs that may be in progress at a time.
 */
public class LoadTestLauncher extends AbstractExecutionThreadService {

  private static final Logger log = LoggerFactory.getLogger(AccessTokenProvider.class.getName());
  private final ListeningExecutorService executor;
  private final RateLimiter rateLimiter;
  private final Semaphore outstandingTestLimiter;
  private final Supplier<Runnable> loadTestRunSupplier;

  public LoadTestLauncher(
      Supplier<Runnable> loadTestRunSupplier,
      double initialLoadTestRate,
      int maxLoadTestOutstandingCount,
      ListeningExecutorService executor) throws IOException, GeneralSecurityException {

    this.loadTestRunSupplier = Preconditions.checkNotNull(loadTestRunSupplier);

    Preconditions.checkArgument(initialLoadTestRate > 0,
        "Must have a positive rate at which load tests are run.");
    rateLimiter = RateLimiter.create(initialLoadTestRate);

    Preconditions.checkArgument(maxLoadTestOutstandingCount > 0,
        "Must allow at least one load test to run at a time.");
    outstandingTestLimiter = new Semaphore(maxLoadTestOutstandingCount, false);
    this.executor = executor;

    log.info("Launcher started at %f QPS", rateLimiter.getRate());
  }

  @Override
  protected void run() {
    while (true) {
      outstandingTestLimiter.acquireUninterruptibly();
      rateLimiter.acquire();
      executor.submit(loadTestRunSupplier.get()).addListener(outstandingTestLimiter::release, executor);
    }
  }

  public double getRate() {
    return rateLimiter.getRate();
  }

  public void setRate(double newRatePerSecond) {
    Preconditions.checkArgument(newRatePerSecond > 0,
        "Must have a positive rate at which load tests are run.");
    rateLimiter.setRate(newRatePerSecond);
  }
}
