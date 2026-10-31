/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.pubsub.flink.internal.sink;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

/** Cancels discarded SDK callback futures when a publisher's executor is forcibly closed. */
public class PubSubPublisherExecutor extends ScheduledThreadPoolExecutor {
  private final Map<RunnableScheduledFuture<?>, Future<?>> callbacks = new ConcurrentHashMap<>();

  public PubSubPublisherExecutor(int threadCount, ThreadFactory threadFactory) {
    super(threadCount, threadFactory);
  }

  @Override
  protected <V> RunnableScheduledFuture<V> decorateTask(
      Runnable callback, RunnableScheduledFuture<V> scheduled) {
    // GAX wraps callback Futures in a separate scheduled Future, so cancel both (verified on
    // google-cloud-pubsub 1.133.1 / GAX 2.55.0; PubSubPublisherShutdownIT guards this).
    if (callback instanceof Future<?> f) callbacks.put(scheduled, f);
    return scheduled;
  }

  @Override
  protected void afterExecute(Runnable task, Throwable failure) {
    callbacks.remove(task);
    super.afterExecute(task, failure);
  }

  @Override
  public List<Runnable> shutdownNow() {
    List<Runnable> discarded = super.shutdownNow();
    for (Runnable task : discarded) {
      Future<?> callback = callbacks.remove(task);
      if (callback != null) callback.cancel(false);
      if (task instanceof Future<?> f) f.cancel(false);
    }
    return discarded;
  }
}
