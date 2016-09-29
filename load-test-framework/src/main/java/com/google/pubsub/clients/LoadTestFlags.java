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

import com.beust.jcommander.Parameter;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Flags used throughout the load test.
 */
public class LoadTestFlags {

  @Parameter(names = {"--initial_test_rate"},
      description = "Number of times per second to try execute a test run.")
  public static final Double initialTestExecutionRate = 1.0;
  @Parameter(names = {"--maximum_test_rate"},
      description = "Maximum number of times per second to execute a test run."
          + " If this flag is not given, the value will be equal to initial_test_rate,"
          + " and the system will run in flat QPS mode.")
  public static final Double maximumTestExecutionRate = -1.0;
  @Parameter(names = {"--rate_change_per_second"},
      description = "Amount at which the test execution rate will change to reach maximum_test_rate.")
  public static final Double rateChangePerSecond = 0.1;
  @Parameter(names = {"--max_objects_creation_inflight"},
      description = "Maximum number of object creations triggered at the same time.")
  public static final Integer maxObjectsCreationInflight = 100;
  @Parameter(names = {"--test_executor_num_threads"},
      description = "Number of threads in the executor that runs all test cases.")
  public static final Integer testExecutorNumThreads = 1000;
  @Parameter(names = {"--project"}, description = "Project to use for load testing.")
  public static final String project = "cloud-pubsub-load-tests";
  @Parameter(names = {"--action_includes_publish"},
      description = "If true, publish one message as part of each test action.")
  public static final Boolean actionIncludesPublish = false;
  @Parameter(names = {"--action_includes_pull"},
      description = "If true, perform pull on created subscriptions.")
  public static final Boolean actionIncludesPull = false;
  @Parameter(names = {"--action_includes_push"},
      description = "If true, start built-in push server in the task to process pushed messages.")
  public static final Boolean actionIncludesPush = false;
  @Parameter(names = {"--return_immediately_on_pull"},
      description = "Value of the return_immediately flag in the pull() call.")
  public static final Boolean returnImmediately = false;
  @Parameter(names = {"--num_topics_per_replica"},
      description = "Number of topics in this task. If set to 0, it will use a global topic.")
  public static final Integer numTopics = 0;
  @Parameter(names = {"--pull_fan_out_factor"}, description = "How many pull subscriptions per topic.")
  public static final Integer pullFanOutFactor = 1;
  @Parameter(names = {"--push_fan_out_factor"}, description = "How many push subscriptions per topic.")
  public static final Integer pushFanOutFactor = 0;
  @Parameter(names = {"--rotation_point"},
      description = "The zero-based index of the load test action, in the rotation of"
          + " all load test actions, that should be run first.")
  public static final Integer rotationPoint = 0;
  @Parameter(names = {"--per_task_name_suffix_format"},
      description = "This string will be appended to the topic and subscription name specified.")
  public static final String perTaskNameSuffixFormat = "-localhost-%s";
  @Parameter(names = {"--load_test_topic_prefix"}, description = "Topic prefix to use for load testing.")
  public static final String loadTestTopicPrefix = "load-test-topic";
  @Parameter(names = {"--load_test_subscription_prefix"},
      description = "Subscription prefix to use for load testing.")
  public static final String loadTestSubscriptionPrefix =
      "load-test-subscription";
  @Parameter(names = {"--push_endpoint"},
      description = "Pubsub push endpoint (relevant only if push_fan_out_factor > 0).")
  public static final String pushEndpoint = "";
  @Parameter(names = {"--port_push"}, description = "Port number of the service that handles the push.")
  public static final Integer portPush = 0;
  @Parameter(names = {"--labels"},
      description = "A map of string key-value-pairs to use as labels on messages. For example, "
          + "--lablels=\"foo=bar,biz=baz\". Empty by default.")
  public static final Map<String, String> labels = new HashMap<>();
  @Parameter(names = {"--publish_batch_size"},
      description = "Number of messages to batch per publish request.")
  public static final Integer publishBatchSize = 100;
  @Parameter(names = {"--pull_batch_size"},
      description = "Number of messages to batch per pull request. "
          + "Ignored if use_batch is set to false.")
  public static final Integer pullBatchSize = 100;
  @Parameter(names = {"--recreate_topics"},
      description = "Whether to re-create existing topics.")
  public static final Boolean recreateTopics = false;
  @Parameter(names = {"--payload_size"},
      description = "Size in bytes of the data field per message")
  public static final Integer payloadSize = 800;
  @Parameter(names = {"--num_modify_ack_deadline"},
      description = "Number of ModifyAckDeadline requests per pull request")
  public static final Integer numModifyAckDeadline = 0;
  @Parameter(names = {"--modify_ack_wait_time"},
      description = "How long to wait before sending modifyAckDeadline request.")
  public static final Duration modifyAckWaitTime = Duration.ofSeconds(5);
  @Parameter(names = {"--modify_ack_deadline_seconds"},
      description = "The new ack deadline with respect to the time modifyAckDeadline request was sent.")
  public static final Integer modifyAckDeadlineSeconds = 10;
  @Parameter(names = {"--start_delay_seconds"},
      description = "The number of seconds to wait after creating topics/subscriptions to perform the "
          + "operations in the load test.")
  public static final Integer startDelaySeconds = 0;

  private LoadTestFlags() {
  } // Don't instantiate
}
