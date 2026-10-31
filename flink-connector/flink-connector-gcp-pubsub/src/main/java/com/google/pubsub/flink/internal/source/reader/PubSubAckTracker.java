/*
 * Copyright 2023 Google LLC
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
package com.google.pubsub.flink.internal.source.reader;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

public class PubSubAckTracker implements AckTracker {
  @GuardedBy("this")
  private final Map<String, Delivery> outstandingAcks = new LinkedHashMap<>();

  private final LongSupplier nanoTime;
  private final Counter redeliveries;
  private final Counter checkpointAckFailures;

  public PubSubAckTracker() {
    this(new UnregisteredMetricsGroup());
  }

  public PubSubAckTracker(MetricGroup group) {
    this(group, System::nanoTime);
  }

  PubSubAckTracker(MetricGroup group, LongSupplier nanoTime) {
    this.nanoTime = nanoTime;
    redeliveries = group.counter("redeliveries", new ThreadSafeSimpleCounter());
    checkpointAckFailures = group.counter("checkpointAckFailures", new ThreadSafeSimpleCounter());
    group.gauge("outstandingMessages", (Gauge<Integer>) this::outstandingMessages);
    group.gauge("outstandingDeliveryHandles", (Gauge<Integer>) this::outstandingDeliveryHandles);
    group.gauge("pendingMessages", (Gauge<Integer>) this::pendingMessages);
    group.gauge("stagedMessages", (Gauge<Integer>) this::stagedMessages);
    group.gauge("checkpointAckBatches", (Gauge<Integer>) this::checkpointAckBatches);
    group.gauge("oldestOutstandingAgeMillis", (Gauge<Long>) this::oldestOutstandingAgeMillis);
  }

  @GuardedBy("this")
  private final Set<String> pendingAcks = new HashSet<>();

  @GuardedBy("this")
  private final List<String> stagedAcks = new ArrayList<>();

  @GuardedBy("this")
  private final SortedMap<Long, Deque<String>> checkpoints = new TreeMap<>();

  @Override
  public synchronized boolean addPendingAck(String messageId, AckReplyConsumer ackReplyConsumer) {
    Delivery delivery = outstandingAcks.get(messageId);
    if (delivery == null) {
      outstandingAcks.put(messageId, new Delivery(ackReplyConsumer, nanoTime.getAsLong()));
      pendingAcks.add(messageId);
      return true;
    }
    // Keep every handle until checkpoint or shutdown; see AckTracker#addPendingAck.
    delivery.add(ackReplyConsumer);
    redeliveries.inc();
    return false;
  }

  @Override
  public synchronized void stagePendingAck(String messageId) {
    if (pendingAcks.remove(messageId)) {
      stagedAcks.add(messageId);
    }
  }

  @Override
  public synchronized void addCheckpoint(long checkpointId) {
    checkpoints.put(checkpointId, new ArrayDeque<>(stagedAcks));
    stagedAcks.clear();
  }

  @Override
  public synchronized void notifyCheckpointComplete(long checkpointId) {
    while (!checkpoints.isEmpty() && checkpoints.firstKey() <= checkpointId) {
      long completed = checkpoints.firstKey();
      Deque<String> batch = checkpoints.get(completed);
      try {
        // Remove each message only after its ack succeeds, so a retry never acks it twice.
        for (String messageId; (messageId = batch.peek()) != null; batch.poll()) {
          outstandingAcks.get(messageId).resolve(true);
          outstandingAcks.remove(messageId);
        }
      } catch (RuntimeException | Error failure) {
        checkpointAckFailures.inc();
        throw failure;
      }
      checkpoints.remove(completed);
    }
  }

  @Override
  public synchronized void nackAll() {
    outstandingAcks.values().forEach(delivery -> delivery.resolve(false));
    outstandingAcks.clear();
    pendingAcks.clear();
    stagedAcks.clear();
    checkpoints.clear();
  }

  private synchronized int outstandingMessages() {
    return outstandingAcks.size();
  }

  private synchronized int outstandingDeliveryHandles() {
    return outstandingAcks.values().stream().mapToInt(Delivery::size).sum();
  }

  private synchronized int pendingMessages() {
    return pendingAcks.size();
  }

  private synchronized int stagedMessages() {
    return stagedAcks.size();
  }

  private synchronized int checkpointAckBatches() {
    return checkpoints.size();
  }

  private synchronized long oldestOutstandingAgeMillis() {
    return outstandingAcks.isEmpty()
        ? 0
        : TimeUnit.NANOSECONDS.toMillis(
            nanoTime.getAsLong() - outstandingAcks.values().iterator().next().receivedNanos);
  }

  private static final class Delivery {
    // Almost every message has a single handle, so the deque and set are allocated lazily.
    private AckReplyConsumer only;
    private Deque<AckReplyConsumer> consumers;
    private Set<AckReplyConsumer> handles;
    final long receivedNanos;

    Delivery(AckReplyConsumer consumer, long receivedNanos) {
      add(consumer);
      this.receivedNanos = receivedNanos;
    }

    int size() {
      if (consumers != null) return consumers.size();
      return only == null ? 0 : 1;
    }

    void add(AckReplyConsumer consumer) {
      if (consumers == null) {
        if (only == null || only == consumer) {
          only = consumer;
          return;
        }
        consumers = new ArrayDeque<>();
        handles = Collections.newSetFromMap(new IdentityHashMap<>());
        consumers.addFirst(only);
        handles.add(only);
        only = null;
      }
      if (!handles.add(consumer)) {
        // The same handle can arrive again; keep one copy at its newest position.
        consumers.removeIf(existing -> existing == consumer);
      }
      consumers.addFirst(consumer);
    }

    void resolve(boolean acknowledge) {
      if (consumers == null) {
        if (only != null) {
          settle(only, acknowledge);
          only = null;
        }
        return;
      }
      while (!consumers.isEmpty()) {
        AckReplyConsumer consumer = consumers.peekFirst();
        settle(consumer, acknowledge);
        consumers.removeFirst();
        handles.remove(consumer);
      }
    }

    private static void settle(AckReplyConsumer consumer, boolean acknowledge) {
      if (acknowledge) {
        consumer.ack();
      } else {
        consumer.nack();
      }
    }
  }
}
