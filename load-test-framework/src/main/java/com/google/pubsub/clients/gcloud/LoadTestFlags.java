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

/**
 * Flags used throughout the load test.
 */
class LoadTestFlags {
  static double executionRate = 1.0;
  static int testExecutorNumThreads = 1000;
  static String project = "project";
  static String topic = "load-test-topic";
  static String subscription = "load-test-subscription"; // set null for Publish
  static int batchSize = 100;
  static int payloadSize = 800;

  // Don't instantiate
  private LoadTestFlags() {
  }

  static void parse(LoadTest loadTest) {
    executionRate = loadTest.executionRate;
    testExecutorNumThreads = loadTest.testExecutorNumThreads;
    project = loadTest.project;
    topic = loadTest.topic;
    subscription = loadTest.subscription;
    batchSize = loadTest.batchSize;
    payloadSize = loadTest.payloadSize;
  }
}
