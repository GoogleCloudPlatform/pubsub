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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
  @Parameter(names = {"--initial_test_rate"},
      description = "Number of times per second to try execute a test run.")
  double initialTestExecutionRate = 1.0;
  @Parameter(names = {"--maximum_test_rate"},
      description = "Maximum number of times per second to execute a test run."
          + " If this flag is not given, the value will be equal to initial_test_rate,"
          + " and the system will run in flat QPS mode.")
  double maximumTestExecutionRate = -1.0;
  @Parameter(names = {"--rate_change_per_second"},
      description = "Amount at which the test execution rate will change to reach maximum_test_rate.")
  double rateChangePerSecond = 0.1;
  @Parameter(names = {"--max_objects_creation_inflight"},
      description = "Maximum number of object creations triggered at the same time.")
  int maxObjectsCreationInflight = 100;
  @Parameter(names = {"--test_executor_num_threads"},
      description = "Number of threads in the executor that runs all test cases.")
  int testExecutorNumThreads = 1000;
  @Parameter(names = {"--project"}, description = "Project to use for load testing.")
  String project = "cloud-pubsub-load-tests";
  @Parameter(names = {"--action_includes_publish"},
      description = "If true, publish one message as part of each test action.")
  boolean actionIncludesPublish = false;
  @Parameter(names = {"--action_includes_pull"},
      description = "If true, perform pull on created subscriptions.")
  boolean actionIncludesPull = false;
  @Parameter(names = {"--return_immediately_on_pull"},
      description = "Value of the return_immediately flag in the pull() call.")
  boolean returnImmediately = false;
  @Parameter(names = {"--num_topics_per_replica"},
      description = "Number of topics in this task. If set to 0, it will use a global topic.")
  int numTopics = 0;
  @Parameter(names = {"--pull_fan_out_factor"}, description = "How many pull subscriptions per topic.")
  int pullFanOutFactor = 1;
  @Parameter(names = {"--rotation_point"},
      description = "The zero-based index of the load test action, in the rotation of"
          + " all load test actions, that should be run first.")
  int rotationPoint = 0;
  @Parameter(names = {"--per_task_name_suffix_format"},
      description = "This string will be appended to the topic and subscription name specified.")
  String perTaskNameSuffixFormat = "-localhost-%s";
  @Parameter(names = {"--load_test_topic_prefix"}, description = "Topic prefix to use for load testing.")
  String loadTestTopicPrefix = "load-test-topic";
  @Parameter(names = {"--load_test_subscription_prefix"},
      description = "Subscription prefix to use for load testing.")
  String loadTestSubscriptionPrefix = "load-test-subscription";
  @Parameter(names = {"--label"},
      description = "A key-value-pair to use as labels on messages, can be repeated. For example, "
          + "--label=\"foo=bar\" --label=\"biz=baz\". Empty by default.")
  List<String> labels = new ArrayList<>();
  @Parameter(names = {"--publish_batch_size"},
      description = "Number of messages to batch per publish request.")
  int publishBatchSize = 100;
  @Parameter(names = {"--pull_batch_size"},
      description = "Number of messages to batch per pull request. "
          + "Ignored if use_batch is set to false.")
  int pullBatchSize = 100;
  @Parameter(names = {"--recreate_topics"},
      description = "Whether to re-create existing topics.")
  boolean recreateTopics = false;
  @Parameter(names = {"--payload_size"},
      description = "Size in bytes of the data field per message")
  int payloadSize = 800;
  @Parameter(names = {"--num_modify_ack_deadline"},
      description = "Number of ModifyAckDeadline requests per pull request")
  int numModifyAckDeadline = 0;
  @Parameter(names = {"--modify_ack_wait_seconds"},
      description = "How long to wait before sending modifyAckDeadline request.")
  int modifyAckWaitSeconds = 5;
  @Parameter(names = {"--modify_ack_deadline_seconds"},
      description = "The new ack deadline with respect to the time modifyAckDeadline request was sent.")
  int modifyAckDeadlineSeconds = 10;
  @Parameter(names = {"--start_delay_seconds"},
      description = "The number of seconds to wait after creating topics/subscriptions to perform the "
          + "operations in the load test.")
  int startDelaySeconds = 0;

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(UncaughtExceptionHandlers.systemExit());
    LoadTest loadTest = new LoadTest();
    new JCommander(loadTest, args);
    LoadTestFlags.parse(loadTest);
    loadTest.run();
  }

  private void parseFlags() {

  }

  private void run() throws Exception {

    final PubSub pubSub = PubSubOptions.builder().projectId(LoadTestFlags.project).build().service();

    ListeningExecutorService executor = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(LoadTestFlags.testExecutorNumThreads));

    log.info(
        "Configured executor with " + LoadTestFlags.testExecutorNumThreads + " threads.");
    final ObjectRepository objectRepository = new ObjectRepository(pubSub, true);

    log.info("Preparing all test resources");
    final AtomicInteger index = new AtomicInteger(LoadTestFlags.rotationPoint);
    final List<RunParams> params =
        RunParams.generatePrototypeParams(objectRepository, executor);
    for (RunParams param : params) {
      log.info("Configured load test run: " + param);
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

    Thread.sleep(LoadTestFlags.startDelayDuration.getSeconds() * 1000);
    loadTestPacer.startAsync().awaitRunning();
  }
}
