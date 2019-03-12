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

import com.google.common.collect.ImmutableList;

import java.util.List;

public class CoreScalingTestParameterProvider implements TestParameterProvider {
  private final TestParameters base;

  CoreScalingTestParameterProvider(TestParameters base) {
    this.base = base;
  }

  @Override
  public List<TestParameters> parameters() {
    return ImmutableList.of(
        base.toBuilder().setNumCoresPerWorker(1).build(),
        base.toBuilder().setNumCoresPerWorker(2).build(),
        base.toBuilder().setNumCoresPerWorker(4).build(),
        base.toBuilder().setNumCoresPerWorker(8).build(),
        base.toBuilder().setNumCoresPerWorker(16).build());
  }
}
