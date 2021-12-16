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

package com.google.pubsub.flic;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.EnumConverter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.flic.common.LatencyTracker;
import com.google.pubsub.flic.common.LoadtestProto.MessageIdentifier;
import com.google.pubsub.flic.common.MessageTracker;
import com.google.pubsub.flic.common.StatsUtils;
import com.google.pubsub.flic.controllers.*;
import com.google.pubsub.flic.controllers.test_parameters.ParameterOverrides;
import com.google.pubsub.flic.controllers.test_parameters.TestParameterProvider;
import com.google.pubsub.flic.controllers.test_parameters.TestParameterProviderConverter;
import com.google.pubsub.flic.controllers.test_parameters.TestParameters;
import com.google.pubsub.flic.output.CsvOutput;
import com.google.pubsub.flic.output.ResultsOutput;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Drives the execution of the framework through command line arguments. */
@Parameters(separators = "=")
public class Driver {
  private static final Logger log = LoggerFactory.getLogger(Driver.class);
  private final List<ResultsOutput> outputMethods;

  @Parameter(
      names = {"--help"},
      help = true)
  public boolean help = false;

  private static class MessagingTypeConverter extends EnumConverter<ClientType.MessagingType> {
    public MessagingTypeConverter(String s, Class<ClientType.MessagingType> aClass) {
      super(s, aClass);
    }
  }

  private static class LanguageConverter extends EnumConverter<ClientType.Language> {
    public LanguageConverter(String s, Class<ClientType.Language> aClass) {
      super(s, aClass);
    }
  }

  @Parameter(
      names = {"--project"},
      required = true,
      description = "Google Cloud Platform project name.")
  private String project = "";

  @Parameter(
      names = {"--zone"},
      description = "The GCE zone in which to create client instances.")
  private String zone = "us-central1-a";

  @Parameter(
      names = {"--messaging_type"},
      description = "Messaging type to use.",
      converter = MessagingTypeConverter.class)
  private ClientType.MessagingType type = ClientType.MessagingType.CPS_GCLOUD;

  @Parameter(
      names = {"--language"},
      description = "Language to test in.",
      converter = LanguageConverter.class)
  private ClientType.Language language = ClientType.Language.JAVA;

  @Parameter(
      names = {"--test_parameters"},
      required = true,
      description =
          "TestParameters to run with, one of 'latency', 'throughput', 'core-scaling', 'thread-scaling' or 'noop'.",
      converter = TestParameterProviderConverter.class)
  private TestParameterProvider testParameterProvider = null;

  private Controller controller;

  private Driver() {
    outputMethods = ImmutableList.of(new CsvOutput());
  }

  public static void main(String[] args) {
    Driver driver = new Driver();
    ParameterOverrides overrides = new ParameterOverrides();
    JCommander jCommander =
        JCommander.newBuilder()
            .addObject(driver)
            .addObject(overrides)
            .build();
    jCommander.parse(args);
    if (driver.help) {
      jCommander.usage();
      return;
    }
    if (driver.testParameterProvider == null) {
      log.error("You must provide a value to the --testParameters flag to run a loadtest.");
    }
    List<TestParameters> testParameters = driver.testParameterProvider.parameters();
    HashMap<TestParameters, Map<ClientType, LatencyTracker>> results = new HashMap<>();
    testParameters.forEach(
        parameters -> {
          TestParameters updated = overrides.apply(parameters);
          Map<ClientType, LatencyTracker> result;
          if (overrides.local) {
            result =
                driver.run(
                    updated,
                    (project, clientParamsMap) ->
                        LocalController.newLocalController(
                            project, clientParamsMap, Executors.newScheduledThreadPool(500)));
          } else if (overrides.inProcess) {
            result =
                driver.run(
                    updated,
                    (project, clientParamsMap) ->
                        InprocessController.newController(
                            project,
                            ImmutableList.copyOf(clientParamsMap.keySet()),
                            Executors.newScheduledThreadPool(20)));
          } else {
            result =
                driver.run(
                    updated,
                    (project, clientParamsMap) ->
                        GCEController.newGCEController(
                            project, clientParamsMap, Executors.newScheduledThreadPool(500)));
          }
          driver.printStats(updated, result);
          results.put(updated, result);
        });
    List<ResultsOutput.TrackedResult> trackedResults =
        results.entrySet().stream()
            .flatMap(
                entry ->
                    entry.getValue().entrySet().stream()
                        .map(
                            typeAndTracker ->
                                new ResultsOutput.TrackedResult(
                                    entry.getKey(),
                                    typeAndTracker.getKey(),
                                    typeAndTracker.getValue())))
            .collect(Collectors.toList());
    driver.outputStats(trackedResults);
    System.exit(0);
  }

  private void outputStats(List<ResultsOutput.TrackedResult> results) {
    outputMethods.forEach(output -> output.outputStats(results));
  }

  private Map<ClientType, LatencyTracker> run(
      TestParameters testParameters,
      BiFunction<String, Map<ClientParams, Integer>, Controller> controllerFunction) {
    Map<ClientParams, Integer> clientParamsMap = new HashMap<>();
    ClientParams.Builder params = ClientParams.builder();
    params.setTestParameters(testParameters);
    params.setProject(project);
    params.setZone(zone);

    ClientType publisherType = new ClientType(type, language, ClientType.MessagingSide.PUBLISHER);
    ClientType subscriberType = new ClientType(type, language, ClientType.MessagingSide.SUBSCRIBER);

    clientParamsMap.put(
        params.setClientType(publisherType).build(), testParameters.numPublisherWorkers());
    clientParamsMap.put(
        params.setClientType(subscriberType).build(), testParameters.numSubscriberWorkers());
    try {
      controller = controllerFunction.apply(project, clientParamsMap);
      if (controller == null) {
        System.exit(1);
      }

      return runTest();
    } catch (Throwable t) {
      log.error("An error occurred...", t);
      controller.stop();
      System.exit(1);
      return null;
    } finally {
      if (controller != null) {
        controller.stop();
      }
    }
  }

  private Map<ClientType, LatencyTracker> runTest() throws Throwable {
    MessageTracker messageTracker = new MessageTracker();
    controller.start(messageTracker);
    log.info(
        "Will start burn-in time in "
            + ((Timestamps.toMillis(controller.getStartTime()) - System.currentTimeMillis()) / 1000)
            + " seconds.");
    // Wait for the load test to finish.
    controller.waitForClients();
    Map<ClientType, LatencyTracker> trackers = controller.getClientLatencyTrackers();
    Iterable<MessageIdentifier> missing = messageTracker.getMissing();
    if (missing.iterator().hasNext()) {
      log.error("Some published messages were not received!\nExamples:");
      int missing_count = 0;
      for (MessageIdentifier identifier : missing) {
        if (missing_count < 5) {
          log.error(
              String.format(
                  "%d:%d", identifier.getPublisherClientId(), identifier.getSequenceNumber()));
        }
        missing_count++;
      }
      log.error("Missing " + missing_count + " total messages.");
    }

    return trackers;
  }

  private void printStats(TestParameters testParameters, Map<ClientType, LatencyTracker> trackers) {
    log.info("===============================================");
    trackers.forEach(
        (type, tracker) -> {
          log.info("Results for " + type + ":");
          log.info("50%: " + tracker.getNthPercentile(50.0));
          log.info("99%: " + tracker.getNthPercentile(99.0));
          log.info("99.9%: " + tracker.getNthPercentile(99.9));
          log.info(
              "Average throughput: "
                  + new DecimalFormat("#.##")
                      .format(
                          StatsUtils.getQPS(tracker.getCount(), testParameters.loadtestDuration())
                              * testParameters.messageSize()
                              / 1000000.0)
                  + " MB/s");
        });
    log.info("===============================================");
  }
}
