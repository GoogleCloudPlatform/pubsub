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
package com.google.pubsub.flic.processing;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Custom ThreadPoolExecutor which tracks how long tasks wait in the task queue before they are
 * actually run.
 */
public class DelayTrackingThreadPool extends ThreadPoolExecutor {

  private Map<Runnable, AtomicLong> taskWaitTime = new ConcurrentHashMap<>();
  private AtomicLong totalWaitTime = new AtomicLong(0);
  private AtomicLong totalTasks = new AtomicLong(0);

  public DelayTrackingThreadPool(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    // Called when the task gets popped off the queue and gets run.
    long waitTime = System.currentTimeMillis() - taskWaitTime.get(r).longValue();
    totalWaitTime.addAndGet(waitTime);
    super.beforeExecute(t, r);
  }

  @Override
  public void execute(Runnable command) {
    // Called when the task gets put on the queue.
    taskWaitTime.put(command, new AtomicLong(System.currentTimeMillis()));
    totalTasks.incrementAndGet();
    super.execute(command);
  }

  /**
   * Computes the average wait time for a task by simply dividing the total wait time by the
   * number of tasks that were run.
   */
  public double getAverageTaskWaitTime() {
    return totalWaitTime.doubleValue() / totalTasks.doubleValue();
  }
}
