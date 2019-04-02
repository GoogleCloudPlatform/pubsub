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

package com.google.pubsub.flic.controllers.test_parameters;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.protobuf.util.Durations;

@Parameters(separators = "=")
public class ParameterOverrides {
  @Parameter(
      names = {"--local"},
      description = "Run the test locally.")
  public boolean local = false;

  @Parameter(
      names = {"--in_process"},
      description = "Run the test in process, java only.")
  public boolean inProcess = false;

  @Parameter(
      names = {"--message_size"},
      description = "Set the message size for test.")
  private Integer messageSize = null;

  @Parameter(
      names = {"--burn_in_minutes"},
      description = "Set the number of minutes to burn in for.")
  private Integer burnInMinutes = null;

  @Parameter(
      names = {"--test_minutes"},
      description = "Set the number of minutes to test for.")
  private Integer testMinutes = null;

  @Parameter(
      names = {"--num_cores"},
      description = "Set the number of cores per worker.")
  private Integer numCores = null;

  @Parameter(
      names = {"--scaling_factor"},
      description = "Set the subscriber scaling factor per core per worker.")
  private Integer scalingFactor = null;

  public TestParameters apply(TestParameters source) {
    TestParameters.Builder builder = source.toBuilder();
    if (messageSize != null) {
      builder.setMessageSize(messageSize);
    }
    if (burnInMinutes != null) {
      builder.setBurnInDuration(Durations.fromSeconds(burnInMinutes * 60));
    }
    if (testMinutes != null) {
      builder.setLoadtestDuration(Durations.fromSeconds(testMinutes * 60));
    }
    if (numCores != null) {
      builder.setNumCoresPerWorker(numCores);
    }
    if (scalingFactor != null) {
      builder.setSubscriberCpuScaling(scalingFactor);
    }
    return builder.build();
  }
}
