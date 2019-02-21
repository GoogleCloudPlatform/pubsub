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

import com.beust.jcommander.IStringConverter;

public class TestParameterProviderConverter implements IStringConverter<TestParameterProvider> {
  @Override
  public TestParameterProvider convert(String value) {
    switch (value) {
      case "latency":
        return TestParameterProvider.of(StandardParameters.LATENCY);
      case "throughput":
        return TestParameterProvider.of(StandardParameters.THROUGHPUT);
      case "core-scaling":
        return new CoreScalingTestParameterProvider(StandardParameters.THROUGHPUT);
      case "message-size":
        return new MessageSizeScalingTestParameterProvider(StandardParameters.THROUGHPUT);
      case "thread-scaling":
        return new ScalingFactorTestParameterProvider(StandardParameters.THROUGHPUT);
      case "noop":
        return TestParameterProvider.of(StandardParameters.NOOP);
    }
    return null;
  }
}
