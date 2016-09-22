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
package com.google.pubsub.flic.clients;

import com.google.common.util.concurrent.RateLimiter;
import com.google.pubsub.flic.processing.DelayTrackingThreadPool;
import com.google.pubsub.flic.task.Task;
import com.google.pubsub.flic.task.TaskArgs;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CPSTask extends Task {

  // Manages the thread pool for the async callbacks.
  protected DelayTrackingThreadPool callbackExecutor;
  // Manages rate limiting.
  protected RateLimiter rateLimiter;

  public CPSTask(TaskArgs args) {
    super(args);
    callbackExecutor =
        new DelayTrackingThreadPool(
            args.getNumResponseThreads(),
            args.getNumResponseThreads(),
            60L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());
  }
}
