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
package com.google.pubsub.flink.internal.source.reader;

import static org.junit.Assert.assertEquals;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Test;

/** Checks acknowledgement ownership against a record-based model, without broker assumptions. */
public class PubSubAckTrackerStateMachineTest {
  @Test
  public void reorderedDeliveriesCheckpointsAndShutdown_matchReferenceModel() {
    for (long seed : new long[] {1, 7, 42, 101, 987654321}) {
      verifySequence(seed);
    }
  }

  private void verifySequence(long seed) {
    Random random = new Random(seed);
    PubSubAckTracker tracker = new PubSubAckTracker();
    // Each record owns all its deliveries until a checkpoint covering its emission completes.
    // This model has no checkpoint queues or staged-ack lists, unlike the implementation.
    Map<String, Record> records = new HashMap<>();
    Map<String, Integer> expected = new HashMap<>();
    Map<String, Integer> actual = new HashMap<>();
    List<Long> checkpoints = new ArrayList<>();
    long nextCheckpoint = 1;
    for (int step = 0; step < 2000; step++) {
      String context = "seed=" + seed + " step=" + step;
      String id = "message-" + random.nextInt(16);
      switch (random.nextInt(7)) {
        case 0:
        case 1:
          String token = "delivery-" + step;
          boolean first = !records.containsKey(id);
          Record record = records.computeIfAbsent(id, unused -> new Record());
          record.deliveries.add(token);
          assertEquals(
              context + " first delivery",
              first,
              tracker.addPendingAck(id, consumer(token, actual)));
          break;
        case 2:
          tracker.stagePendingAck(id);
          if (records.containsKey(id)) {
            records.get(id).emitted = true;
          }
          break;
        case 3:
          tracker.addCheckpoint(nextCheckpoint);
          for (Record pending : records.values()) {
            if (pending.emitted && pending.checkpoint == null) {
              pending.checkpoint = nextCheckpoint;
            }
          }
          checkpoints.add(nextCheckpoint++);
          break;
        case 4:
        case 5:
          if (!checkpoints.isEmpty()) {
            // Completion callbacks may skip boundaries, repeat, or arrive late after shutdown.
            long completed = checkpoints.get(random.nextInt(checkpoints.size()));
            Iterator<Record> iterator = records.values().iterator();
            while (iterator.hasNext()) {
              Record committed = iterator.next();
              if (committed.checkpoint != null && committed.checkpoint <= completed) {
                for (String delivery : committed.deliveries) {
                  expected.merge("ack:" + delivery, 1, Integer::sum);
                }
                iterator.remove();
              }
            }
            tracker.notifyCheckpointComplete(completed);
          }
          break;
        default:
          for (Record outstanding : records.values()) {
            for (String delivery : outstanding.deliveries) {
              expected.merge("nack:" + delivery, 1, Integer::sum);
            }
          }
          records.clear();
          tracker.nackAll();
      }
      assertEquals(context + " acknowledgement ownership", expected, actual);
    }
    for (Record outstanding : records.values()) {
      for (String delivery : outstanding.deliveries) {
        expected.merge("nack:" + delivery, 1, Integer::sum);
      }
    }
    tracker.nackAll();
    assertEquals("seed=" + seed + " final shutdown", expected, actual);
  }

  private AckReplyConsumer consumer(String delivery, Map<String, Integer> events) {
    return new AckReplyConsumer() {
      @Override
      public void ack() {
        events.merge("ack:" + delivery, 1, Integer::sum);
      }

      @Override
      public void nack() {
        events.merge("nack:" + delivery, 1, Integer::sum);
      }
    };
  }

  private static class Record {
    final List<String> deliveries = new ArrayList<>();
    boolean emitted;
    Long checkpoint;
  }
}
