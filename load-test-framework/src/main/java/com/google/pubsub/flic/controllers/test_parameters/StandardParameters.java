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

import com.google.protobuf.util.Durations;
import java.util.Optional;

public class StandardParameters {
  public static TestParameters LATENCY =
      TestParameters.builder()
          .setMessageSize(1)
          .setPublishBatchSize(1)
          .setPublishRatePerSec(Optional.of(1))
          .setPublishBatchDuration(Durations.fromMillis(1))
          .setNumCoresPerWorker(1)
          .build();
  public static TestParameters THROUGHPUT =
      TestParameters.builder().setNumCoresPerWorker(16).build();
  public static TestParameters NOOP =
      TestParameters.builder()
          .setBurnInDuration(Durations.fromSeconds(0))
          .setLoadtestDuration(Durations.fromSeconds(0))
          .setNumCoresPerWorker(1)
          .build();
}
