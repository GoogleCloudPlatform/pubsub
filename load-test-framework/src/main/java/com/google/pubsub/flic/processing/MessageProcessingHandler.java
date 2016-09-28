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

import com.google.pubsub.flic.common.MessagePacketProto.MessagePacket;
import com.google.pubsub.flic.common.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds all necessary abstractions in order to process message data for correctness and two
 * different types of latency.
 */
public class MessageProcessingHandler {

  // If the buffer gets to a size of a million, flush it.
  private static final int BUFFER_FLUSH_SIZE = 1000000;
  private static final String LATENCY_STATS_FORMAT =
      " (min, max, avg, 50%%, 95%%, 99%%) = %d, %d, %.1f, %d, %d, %d (ms)";
  private static final Logger log =
      LoggerFactory.getLogger(MessageProcessingHandler.class.getName());

  // The file in which to dump the data.
  private File filedump;
  // The total number of items that need to be processed.
  private int totalItems;
  // Buffers data until the number of items buffered is equal to totalItems.
  private List<MessagePacket> buffer;
  // The type of latency the stats pertain to.
  private LatencyType latencyType;
  // Assists with condition waiting,
  private LockingHelper lockHelper;
  private AtomicLong throughputStats = new AtomicLong(0);
  private Histogram latencyStats = new ConcurrentHistogram(1, 10000000, 2);

  /** Indicates to the user what type of latency the stats pertain to. */
  public enum LatencyType {
    PUB_TO_ACK("Pub-to-Ack"),
    END_TO_END("End-to-End");

    private String name;

    LatencyType(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  public MessageProcessingHandler(int totalItems) {
    this.totalItems = totalItems;
    buffer = new ArrayList<>(totalItems);
    lockHelper = new LockingHelper(totalItems);
  }

  public void setFiledump(File filedump) {
    this.filedump = filedump;
  }

  public File getFiledump() {
    return filedump;
  }

  public int getTotalItems() {
    return totalItems;
  }

  public void setLatencyType(LatencyType latencyType) {
    this.latencyType = latencyType;
  }

  /** Adds latency and throughput stats as well as decrementing the count for a barrier. */
  public synchronized void addStats(int num, long latency, long size) {
    if (lockHelper.barrier.getCount() != 0) {
      for (int i = 0; i < num; ++i) {
        latencyStats.recordValue(latency);
        lockHelper.barrier.countDown();
        wakeUpCondition();
      }
      throughputStats.addAndGet(size);
    }
  }

  /**
   * Prints the results of the stats and waits for all stats to be added before printing the
   * results. Print the average time a callback had to wait to be run by a thread in the thread
   * pool. Do not print anything if the task failed for any reason.
   */
  public void printStats(long start, @Nullable DelayTrackingThreadPool executor, AtomicBoolean failureFlag)
      throws Exception {
    lockHelper.conditionLock.lock();
    while (!lockHelper.barrier.await(0, TimeUnit.MICROSECONDS) && !failureFlag.get()) {
      lockHelper.condition.await();
    }
    lockHelper.conditionLock.unlock();
    if (failureFlag.get()) {
      return;
    }
    // Latency stats.
    log.info(
        latencyType.toString()
            + " for "
            + latencyStats.getTotalCount()
            + " messages: "
            + String.format(
                LATENCY_STATS_FORMAT,
                latencyStats.getMinValue(),
                latencyStats.getMaxValue(),
                latencyStats.getMean(),
                latencyStats.getValueAtPercentile(50),
                latencyStats.getValueAtPercentile(95),
                latencyStats.getValueAtPercentile(99)));
    // Print delay on sender side for processing and batching delay in case of CPS, null w/ Kafka
    if(executor != null) {
      log.info(
          "The average delay for processing "
              + latencyType
              + " latency was "
              + executor.getAverageTaskWaitTime()
              + ""
              + " "
              + "ms");
    }
    // Throughput stats.
    double diff = (double) (System.currentTimeMillis() - start) / 1000;
    long averageMessagesPerSec = (long) (getTotalItems() / diff);
    long averageBytesPerSec = (long) (throughputStats.longValue() / diff);
    String averageBytesPerSecString = FileUtils.byteCountToDisplaySize(averageBytesPerSec);
    log.info(
        "Average throughput: "
            + averageMessagesPerSec
            + " messages/sec, "
            + averageBytesPerSecString
            + "/sec");
  }

  public void wakeUpCondition() {
    lockHelper.conditionLock.lock();
    lockHelper.condition.signal();
    lockHelper.conditionLock.unlock();
  }

  /**
   * Creates a {@link MessagePacket} from the passed in data and adds it to {@link #buffer}. When
   * the buffer reaches its capacity, write the contents to a file.
   */
  public synchronized void createMessagePacketAndAdd(String topic, String key, String value)
      throws Exception {
    if (buffer.size() < totalItems) {
      MessagePacket packet =
          MessagePacket.newBuilder()
              .setTopic(topic)
              .setKey(key == null ? "" : key)
              .setValue(value)
              .build();
      buffer.add(packet);
      if (buffer.size() == totalItems || buffer.size() == BUFFER_FLUSH_SIZE) {
        Utils.writeToFile(buffer, filedump);
        buffer.clear();
      }
    }
  }

  /**
   * Logs the progress that a task has made with its execution, given a marker and how many messages
   * that task has processed.
   */
  public static synchronized void displayProgress(AtomicInteger marker, AtomicInteger numMessages) {
    if (numMessages.intValue() == Math.pow(10, marker.intValue())) {
      log.info("Progress: Asynchronously processed " + numMessages + " messages");
      marker.incrementAndGet();
    }
  }

  private class LockingHelper {

    CountDownLatch barrier;
    Lock conditionLock;
    Condition condition;

    public LockingHelper(int totalItems) {
      barrier = new CountDownLatch(totalItems);
      conditionLock = new ReentrantLock();
      condition = conditionLock.newCondition();
    }
  }
}
