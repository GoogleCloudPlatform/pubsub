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

package com.google.pubsub.clients.common;

import com.google.pubsub.flic.common.LoadtestProto;

/**
 * Each task is responsible for triggering its workers when it is run. It controls its own
 * parallelism.
 */
public interface LoadtestTask {
  // Start the task
  void start();

  // Stop the task
  void stop();

  // A factory for constructing a task.
  public interface Factory {
    LoadtestTask newTask(
        LoadtestProto.StartRequest request, MetricsHandler handler, int workerCount);
  }
}
