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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Flags used throughout the load test.
 */
class LoadTestFlags {

  final static Map<String, String> labels = new HashMap<>();
  static double initialTestExecutionRate = 1.0;
  static double maximumTestExecutionRate = -1.0;
  static double rateChangePerSecond = 0.1;
  static int maxObjectsCreationInflight = 100;
  static int testExecutorNumThreads = 1000;
  static String project = "cloud-pubsub-load-tests";
  static boolean actionIncludesPublish = false;
  static boolean actionIncludesPull = false;
  static boolean returnImmediately = false;
  static int numTopics = 0;
  static int pullFanOutFactor = 1;
  static int rotationPoint = 0;
  static String perTaskNameSuffixFormat = "-localhost-%s";
  static String loadTestTopicPrefix = "load-test-topic";
  static String loadTestSubscriptionPrefix = "load-test-subscription";
  static int publishBatchSize = 100;
  static int pullBatchSize = 100;
  static boolean recreateTopics = false;
  static int payloadSize = 800;
  static int numModifyAckDeadline = 0;
  static Duration modifyAckWaitDuration = Duration.ofSeconds(5);
  static Duration modifyAckDeadlineDuration = Duration.ofSeconds(10);
  static Duration startDelayDuration = Duration.ofSeconds(0);

  private LoadTestFlags() {
  } // Don't instantiate

  static void parse(LoadTest loadTest) {
    initialTestExecutionRate = loadTest.initialTestExecutionRate;
    maximumTestExecutionRate = loadTest.maximumTestExecutionRate;
    rateChangePerSecond = loadTest.rateChangePerSecond;
    maxObjectsCreationInflight = loadTest.maxObjectsCreationInflight;
    testExecutorNumThreads = loadTest.testExecutorNumThreads;
    project = loadTest.project;
    actionIncludesPublish = loadTest.actionIncludesPublish;
    actionIncludesPull = loadTest.actionIncludesPull;
    returnImmediately = loadTest.returnImmediately;
    numTopics = loadTest.numTopics;
    pullFanOutFactor = loadTest.pullFanOutFactor;
    rotationPoint = loadTest.rotationPoint;
    perTaskNameSuffixFormat = loadTest.perTaskNameSuffixFormat;
    loadTestTopicPrefix = loadTest.loadTestTopicPrefix;
    loadTestSubscriptionPrefix = loadTest.loadTestSubscriptionPrefix;
    loadTest.labels.forEach((kv) -> {
      String[] pair = kv.split("=", 2);
      labels.put(pair[0], pair[1]);
    });
    publishBatchSize = loadTest.publishBatchSize;
    pullBatchSize = loadTest.pullBatchSize;
    recreateTopics = loadTest.recreateTopics;
    payloadSize = loadTest.payloadSize;
    numModifyAckDeadline = loadTest.numModifyAckDeadline;
    modifyAckWaitDuration = Duration.ofSeconds(loadTest.modifyAckWaitSeconds);
    modifyAckDeadlineDuration = Duration.ofSeconds(loadTest.modifyAckDeadlineSeconds);
    startDelayDuration = Duration.ofSeconds(loadTest.startDelaySeconds);
  }
}
