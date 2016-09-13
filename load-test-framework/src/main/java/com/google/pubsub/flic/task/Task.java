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
package com.google.pubsub.flic.task;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Base class for all tasks. */
public class Task {

  // Arguments for the task.
  protected TaskArgs args;
  // Keeps track of how many messages have been processed.
  protected AtomicInteger messageNo = new AtomicInteger(1);
  // Used for marking progress.
  protected AtomicInteger marker = new AtomicInteger(2);
  // Set to true when a failure occurs during the execution of a task.
  protected AtomicBoolean failureFlag = new AtomicBoolean(false);

  public Task(TaskArgs args) {
    this.args = args;
  }
}
