package com.google.pubsub.clients.common;

import com.google.common.util.concurrent.ForwardingBlockingQueue;

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
