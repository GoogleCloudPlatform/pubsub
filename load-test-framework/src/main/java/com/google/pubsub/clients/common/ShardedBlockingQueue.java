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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

class ShardedBlockingQueue<T> {
  private final AtomicInteger next = new AtomicInteger(0);
  private final ArrayList<BlockingQueue<T>> delegates;

  ShardedBlockingQueue() {
    int cores = Runtime.getRuntime().availableProcessors();
    delegates = new ArrayList<>(cores);
    for (int i = 0; i < cores; i++) {
      delegates.add(new LinkedBlockingQueue<>());
    }
  }

  private BlockingQueue<T> delegate() {
    return delegates.get(next.getAndIncrement() % delegates.size());
  }

  void add(T toAdd) {
    delegate().add(toAdd);
  }

  void drainTo(Collection<? super T> out) {
    for (BlockingQueue<T> delegate : delegates) {
      delegate.drainTo(out);
    }
  }
}
