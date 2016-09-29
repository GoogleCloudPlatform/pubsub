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

import com.beust.jcommander.JCommander;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Uses the {@link LoadTestLauncher} to repeatedly start {@link LoadTestRun LoadTestRuns} to
 * generate load on the server.
 */
public class LoadTest {

  private static final Logger log = LoggerFactory.getLogger(LoadTest.class);

  public static void main(String[] args) throws Exception {
    new JCommander(LoadTestFlags.class).parse(args);
    Thread.setDefaultUncaughtExceptionHandler(UncaughtExceptionHandlers.systemExit());

    final PubSub pubSub = PubSubOptions.builder().projectId(LoadTestFlags.project).build().service();

    ListeningExecutorService executor = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(LoadTestFlags.testExecutorNumThreads));

    log.info(
        "Configured executor with %d threads.", LoadTestFlags.testExecutorNumThreads);
    final ObjectRepository objectRepository = new ObjectRepository(pubSub, true);

    log.info("Preparing all test resources");
    final AtomicInteger index = new AtomicInteger(LoadTestFlags.rotationPoint);
    final List<RunParams> params =
        RunParams.generatePrototypeParams(objectRepository, executor);
    for (RunParams param : params) {
      log.info("Configured load test run: %s", param);
    }

    final char[] payloadArray = new char[LoadTestFlags.payloadSize];
    Arrays.fill(payloadArray, 'A');
    final String payload = payloadArray.toString();

    Supplier<Runnable> loadTestRunSupplier = () -> {
      int currentIndex = index.getAndAdd(1);
      RunParams protoParam = params.get(currentIndex % params.size());
      return new LoadTestRun(
          pubSub, protoParam, currentIndex, payload);
    };

    log.info("Bringing up load test");
    final LoadTestLauncher loadTestLauncher =
        new LoadTestLauncher(
            loadTestRunSupplier,
            LoadTestFlags.initialTestExecutionRate,
            LoadTestFlags.testExecutorNumThreads,
            executor);
    loadTestLauncher.startAsync().awaitRunning();

    double maxExecutionRate = LoadTestFlags.maximumTestExecutionRate < 0
        ? LoadTestFlags.initialTestExecutionRate
        : LoadTestFlags.maximumTestExecutionRate;

    final LoadTestPacer loadTestPacer =
        new LoadTestPacer(
            loadTestLauncher,
            maxExecutionRate,
            LoadTestFlags.rateChangePerSecond);

    Thread.sleep(LoadTestFlags.startDelaySeconds * 1000);
    loadTestPacer.startAsync().awaitRunning();
  }
}
