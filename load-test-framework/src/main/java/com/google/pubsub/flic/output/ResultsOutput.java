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

package com.google.pubsub.flic.output;

import com.google.pubsub.flic.common.LatencyTracker;
import com.google.pubsub.flic.controllers.ClientType;
import com.google.pubsub.flic.controllers.test_parameters.TestParameters;
import java.util.List;

public interface ResultsOutput {
  class TrackedResult {
    public TestParameters testParameters;
    public ClientType type;
    public LatencyTracker tracker;

    public TrackedResult(TestParameters testParameters, ClientType type, LatencyTracker tracker) {
      this.testParameters = testParameters;
      this.type = type;
      this.tracker = tracker;
    }
  }

  void outputStats(List<TrackedResult> results);
}
