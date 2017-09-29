/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

package com.google.pubsub.clients.consumer.ack;

import com.google.api.core.ApiClock;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.batching.FlowController.FlowControlException;
import com.google.api.gax.core.Distribution;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.apache.kafka.common.KafkaException;
import org.threeten.bp.Duration;
import org.threeten.bp.Instant;

/**
 * Dispatches messages to a message receiver while handling the messages acking and lease
 * extensions.
 */
class MessageDispatcher {
  private static final Logger logger = Logger.getLogger(MessageDispatcher.class.getName());

  private static final int INITIAL_ACK_DEADLINE_EXTENSION_SECONDS = 2;
  private static final int MAX_ACK_DEADLINE_EXTENSION_SECS = 10 * 60; // 10m

  private final ScheduledExecutorService systemExecutor;
  private final ApiClock clock;

  private final Duration ackExpirationPadding;
  private final Duration maxAckExtensionPeriod;
  private final MessageReceiver receiver;
  private final AckProcessor ackProcessor;

  private final FlowController flowController;
  private final MessageWaiter messagesWaiter;

  private final PriorityQueue<ExtensionJob> outstandingAckHandlers;
  private final HashMap<String, AckHandler> pendingAcks;
  private final Set<String> pendingNacks;

  private final Lock alarmsLock;
  private final Long retryBackoffMs;
  private int messageDeadlineSeconds;
  private ScheduledFuture<?> ackDeadlineExtensionAlarm;
  private Instant nextAckDeadlineExtensionAlarmTime;
  private ScheduledFuture<?> pendingAcksAlarm;

  private final Deque<OutstandingMessagesBatch> outstandingMessageBatches;

  // To keep track of number of seconds the receiver takes to process messages.
  private final Distribution ackLatencyDistribution;

  // ExtensionJob represents a group of {@code AckHandler}s that shares the same expiration.
  //
  // It is Comparable so that it may be put in a PriorityQueue.
  // For efficiency, it is also mutable, so great care should be taken to make sure
  // it is not modified while inside the queue.
  // The hashcode and equals methods are explicitly not implemented to discourage
  // the use of this class as keys in maps or similar containers.
  private class ExtensionJob implements Comparable<ExtensionJob> {
    Instant creation;
    Instant expiration;
    int nextExtensionSeconds;
    ArrayList<AckHandler> ackHandlers;

    ExtensionJob(
        Instant creation,
        Instant expiration,
        int initialAckDeadlineExtension,
        ArrayList<AckHandler> ackHandlers) {
      this.creation = creation;
      this.expiration = expiration;
      nextExtensionSeconds = initialAckDeadlineExtension;
      this.ackHandlers = ackHandlers;
    }

    void extendExpiration(Instant now) {
      Instant possibleExtension = now.plus(Duration.ofSeconds(nextExtensionSeconds));
      Instant maxExtension = creation.plus(maxAckExtensionPeriod);
      expiration = possibleExtension.isBefore(maxExtension) ? possibleExtension : maxExtension;
      nextExtensionSeconds = Math.min(2 * nextExtensionSeconds, MAX_ACK_DEADLINE_EXTENSION_SECS);
    }

    @Override
    public int compareTo(ExtensionJob other) {
      return expiration.compareTo(other.expiration);
    }

    @Override
    public String toString() {
      ArrayList<String> ackIds = new ArrayList<>();
      for (AckHandler ah : ackHandlers) {
        ackIds.add(ah.ackId);
      }
      return String.format(
          "ExtensionJob {expiration: %s, nextExtensionSeconds: %d, ackIds: %s}",
          expiration, nextExtensionSeconds, ackIds);
    }
  }

  /** Stores the data needed to asynchronously modify acknowledgement deadlines. */
  static class PendingModifyAckDeadline {
    final List<String> ackIds;
    final int deadlineExtensionSeconds;

    PendingModifyAckDeadline(int deadlineExtensionSeconds, String... ackIds) {
      this.ackIds = new ArrayList<>();
      this.deadlineExtensionSeconds = deadlineExtensionSeconds;
      for (String ackId : ackIds) {
        addAckId(ackId);
      }
    }

    void addAckId(String ackId) {
      ackIds.add(ackId);
    }

    @Override
    public String toString() {
      return String.format(
          "PendingModifyAckDeadline{extension: %d sec, ackIds: %s}",
          deadlineExtensionSeconds, ackIds);
    }
  }

  /** Internal representation of a reply to a Pubsub message, to be sent back to the service. */
  public enum AckReply {
    ACK,
    NACK
  }

  /**
   * Handles callbacks for acking/nacking messages from the {@link
   * MessageReceiver}.
   */
  private class AckHandler implements FutureCallback<AckReply> {
    private final String ackId;
    private final long offset;
    private final int outstandingBytes;
    private final AtomicBoolean acked;
    private final long receivedTimeMillis;

    AckHandler(String ackId, int outstandingBytes, long offset) {
      this.ackId = ackId;
      this.outstandingBytes = outstandingBytes;
      this.offset = offset;
      acked = new AtomicBoolean(false);
      receivedTimeMillis = clock.millisTime();
    }

    @Override
    public void onFailure(Throwable t) {
      logger.log(
          Level.WARNING,
          "MessageReceiver failed to processes ack ID: " + ackId + ", the message will be nacked.",
          t);
      acked.getAndSet(true);
      synchronized (pendingNacks) {
        pendingNacks.add(ackId);
      }
      flowController.release(1, outstandingBytes);
      messagesWaiter.incrementPendingMessages(-1);
      processOutstandingBatches();
    }

    @Override
    public void onSuccess(AckReply reply) {
      switch (reply) {
        case ACK:
          synchronized (pendingAcks) {
            pendingAcks.put(ackId, this);
          }
          // Record the latency rounded to the next closest integer.
          ackLatencyDistribution.record(
              Ints.saturatedCast(
                  (long) Math.ceil((clock.millisTime() - receivedTimeMillis) / 1000D)));
          break;
        case NACK:
          synchronized (pendingNacks) {
            pendingNacks.add(ackId);
          }
          break;
        default:
          throw new IllegalArgumentException(String.format("AckReply: %s not supported", reply));
      }
      flowController.release(1, outstandingBytes);
      messagesWaiter.incrementPendingMessages(-1);
      processOutstandingBatches();
    }
  }

  public interface AckProcessor {
    ListenableFuture<List<Empty>> sendAckOperations(
        List<String> acksToSend, List<PendingModifyAckDeadline> ackDeadlineExtensions);
  }

  MessageDispatcher(
      MessageReceiver receiver,
      AckProcessor ackProcessor,
      Duration ackExpirationPadding,
      Duration maxAckExtensionPeriod,
      Distribution ackLatencyDistribution,
      FlowController flowController,
      ScheduledExecutorService systemExecutor,
      ApiClock clock,
      Long retryBackoffMs) {
    this.systemExecutor = systemExecutor;
    this.ackExpirationPadding = ackExpirationPadding;
    this.maxAckExtensionPeriod = maxAckExtensionPeriod;
    this.receiver = receiver;
    this.ackProcessor = ackProcessor;
    this.flowController = flowController;
    this.retryBackoffMs = retryBackoffMs;
    outstandingMessageBatches = new LinkedList<>();
    outstandingAckHandlers = new PriorityQueue<>();
    pendingAcks = new HashMap<>();
    pendingNacks = new HashSet<>();
    // 601 buckets of 1s resolution from 0s to MAX_ACK_DEADLINE_SECONDS
    this.ackLatencyDistribution = ackLatencyDistribution;
    alarmsLock = new ReentrantLock();
    nextAckDeadlineExtensionAlarmTime = Instant.ofEpochMilli(Long.MAX_VALUE);
    messagesWaiter = new MessageWaiter();
    this.clock = clock;
  }

  void stop() {
    messagesWaiter.waitNoMessages();
    alarmsLock.lock();
    try {
      if (ackDeadlineExtensionAlarm != null) {
        ackDeadlineExtensionAlarm.cancel(true);
        ackDeadlineExtensionAlarm = null;
      }
    } finally {
      alarmsLock.unlock();
    }

    extendAckDeadlines(new ArrayList<>());
  }

  void setMessageDeadlineSeconds(int messageDeadlineSeconds) {
    this.messageDeadlineSeconds = messageDeadlineSeconds;
  }

  static class OutstandingMessagesBatch {
    private final Deque<OutstandingMessage> messages;
    private final Runnable doneCallback;

    static class OutstandingMessage {
      private final ReceivedMessage receivedMessage;
      private final AckHandler ackHandler;

      OutstandingMessage(ReceivedMessage receivedMessage, AckHandler ackHandler) {
        this.receivedMessage = receivedMessage;
        this.ackHandler = ackHandler;
      }

      ReceivedMessage receivedMessage() {
        return receivedMessage;
      }

      AckHandler ackHandler() {
        return ackHandler;
      }
    }

    OutstandingMessagesBatch(Runnable doneCallback) {
      this.messages = new LinkedList<>();
      this.doneCallback = doneCallback;
    }

    void addMessage(ReceivedMessage receivedMessage, AckHandler ackHandler) {
      this.messages.add(new OutstandingMessage(receivedMessage, ackHandler));
    }

    public Deque<OutstandingMessage> messages() {
      return messages;
    }
  }

  void processReceivedMessages(List<ReceivedMessage> messages, Runnable doneCallback) {
    if (messages.isEmpty()) {
      doneCallback.run();
      return;
    }
    messagesWaiter.incrementPendingMessages(messages.size());

    OutstandingMessagesBatch outstandingBatch = new OutstandingMessagesBatch(doneCallback);
    final ArrayList<AckHandler> ackHandlers = new ArrayList<>(messages.size());
    for (ReceivedMessage message : messages) {
      long offset = Long.parseLong(message.getMessage().getAttributesOrDefault("offset", "0"));

      AckHandler ackHandler =
          new AckHandler(message.getAckId(), message.getMessage().getSerializedSize(), offset);

      ackHandlers.add(ackHandler);
      outstandingBatch.addMessage(message, ackHandler);
    }

    Instant expiration = Instant.ofEpochMilli(clock.millisTime())
        .plusSeconds(messageDeadlineSeconds);
    synchronized (outstandingAckHandlers) {
      outstandingAckHandlers.add(
          new ExtensionJob(
              Instant.ofEpochMilli(clock.millisTime()),
              expiration,
              INITIAL_ACK_DEADLINE_EXTENSION_SECONDS,
              ackHandlers));
    }
    setupNextAckDeadlineExtensionAlarm(expiration);

    synchronized (outstandingMessageBatches) {
      outstandingMessageBatches.add(outstandingBatch);
    }
    processOutstandingBatches();
  }

  void processOutstandingBatches() {
    while (true) {
      boolean batchDone = false;
      Runnable batchCallback = null;
      OutstandingMessagesBatch.OutstandingMessage outstandingMessage;
      synchronized (outstandingMessageBatches) {
        OutstandingMessagesBatch nextBatch = outstandingMessageBatches.peek();
        if (nextBatch == null) {
          return;
        }
        outstandingMessage = nextBatch.messages.peek();
        if (outstandingMessage == null) {
          return;
        }
        try {
          // This is a non-blocking flow controller.
          flowController.reserve(
              1, outstandingMessage.receivedMessage().getMessage().getSerializedSize());
        } catch (FlowController.MaxOutstandingElementCountReachedException
            | FlowController.MaxOutstandingRequestBytesReachedException flowControlException) {
          return;
        } catch (FlowControlException unexpectedException) {
          throw new IllegalStateException("Flow control unexpected exception", unexpectedException);
        }
        nextBatch.messages.poll(); // We got a hold to the message already.
        batchDone = nextBatch.messages.isEmpty();
        if (batchDone) {
          outstandingMessageBatches.poll();
          batchCallback = nextBatch.doneCallback;
        }
      }

      final PubsubMessage message = outstandingMessage.receivedMessage().getMessage();
      final AckHandler ackHandler = outstandingMessage.ackHandler();
      final SettableFuture<AckReply> response = SettableFuture.create();
      final AckReplyConsumer consumer =
          new AckReplyConsumer() {
            @Override
            public void ack() {
              response.set(AckReply.ACK);
            }

            @Override
            public void nack() {
              response.set(AckReply.NACK);
            }
          };
      Futures.addCallback(response, ackHandler);

      try {
        receiver.receiveMessage(message, consumer);
      } catch (Exception e) {
        response.setException(e);
      }

      if (batchDone) {
        batchCallback.run();
      }
    }
  }

  private class AckDeadlineAlarm implements Runnable {
    @Override
    public void run() {
      alarmsLock.lock();
      try {
        nextAckDeadlineExtensionAlarmTime = Instant.ofEpochMilli(Long.MAX_VALUE);
        ackDeadlineExtensionAlarm = null;
        if (pendingAcksAlarm != null) {
          pendingAcksAlarm.cancel(false);
          pendingAcksAlarm = null;
        }
      } finally {
        alarmsLock.unlock();
      }

      Instant now = Instant.ofEpochMilli(clock.millisTime());
      // Rounded to the next second, so we only schedule future alarms at the second
      // resolution.
      Instant cutOverTime =
          Instant.ofEpochMilli(
              ((long) Math.ceil(now.plus(ackExpirationPadding).plusMillis(500).toEpochMilli() / 1000.0))
                  * 1000L);
      logger.log(
          Level.FINER,
          "Running alarm sent outstanding acks, at time: {0}, with cutover time: {1}, padding: {2}",
          new Object[] {now, cutOverTime, ackExpirationPadding});
      Instant nextScheduleExpiration = null;
      List<PendingModifyAckDeadline> modifyAckDeadlinesToSend = new ArrayList<>();

      // Holding area for jobs we'll put back into the queue
      // so we don't process the same job twice.
      List<ExtensionJob> renewJobs = new ArrayList<>();

      synchronized (outstandingAckHandlers) {
        while (!outstandingAckHandlers.isEmpty()
            && outstandingAckHandlers.peek().expiration.compareTo(cutOverTime) <= 0) {
          ExtensionJob job = outstandingAckHandlers.poll();

          if (maxAckExtensionPeriod.toMillis() > 0
              && job.creation.plus(maxAckExtensionPeriod).compareTo(now) <= 0) {
            // The job has expired, according to the maxAckExtensionPeriod, we are just going to
            // drop it.
            continue;
          }

          // If a message has already been acked, remove it, nothing to do.
          for (int i = 0; i < job.ackHandlers.size(); ) {
            if (job.ackHandlers.get(i).acked.get()) {
              Collections.swap(job.ackHandlers, i, job.ackHandlers.size() - 1);
              job.ackHandlers.remove(job.ackHandlers.size() - 1);
            } else {
              i++;
            }
          }

          if (job.ackHandlers.isEmpty()) {
            continue;
          }

          job.extendExpiration(now);
          long extensionMillis = Duration.between(now, job.expiration).toMillis();
          int extensionSeconds = Ints
              .saturatedCast(TimeUnit.MILLISECONDS.toSeconds(extensionMillis));
          PendingModifyAckDeadline pendingModAckDeadline =
              new PendingModifyAckDeadline(extensionSeconds);
          for (AckHandler ackHandler : job.ackHandlers) {
            pendingModAckDeadline.addAckId(ackHandler.ackId);
          }
          modifyAckDeadlinesToSend.add(pendingModAckDeadline);
          renewJobs.add(job);
        }
        outstandingAckHandlers.addAll(renewJobs);

        if (!outstandingAckHandlers.isEmpty()) {
          nextScheduleExpiration = outstandingAckHandlers.peek().expiration;
        }
      }

      extendAckDeadlines(modifyAckDeadlinesToSend);

      if (nextScheduleExpiration != null) {
        logger.log(
            Level.FINER,
            "Scheduling based on outstanding, at time: {0}, next scheduled time: {1}",
            new Object[] {now, nextScheduleExpiration});
        setupNextAckDeadlineExtensionAlarm(nextScheduleExpiration);
      }
    }
  }

  private void setupNextAckDeadlineExtensionAlarm(Instant expiration) {
    Instant possibleNextAlarmTime = expiration.minus(ackExpirationPadding);
    alarmsLock.lock();
    try {
      if (nextAckDeadlineExtensionAlarmTime.isAfter(possibleNextAlarmTime)) {
        logger.log(
            Level.FINER,
            "Scheduling next alarm time: {0}, previous alarm time: {1}",
            new Object[] {possibleNextAlarmTime, nextAckDeadlineExtensionAlarmTime});
        if (ackDeadlineExtensionAlarm != null) {
          logger.log(Level.FINER, "Canceling previous alarm");
          ackDeadlineExtensionAlarm.cancel(false);
        }

        nextAckDeadlineExtensionAlarmTime = possibleNextAlarmTime;

        ackDeadlineExtensionAlarm =
            systemExecutor.schedule(
                new AckDeadlineAlarm(),
                nextAckDeadlineExtensionAlarmTime.toEpochMilli() - clock.millisTime(),
                TimeUnit.MILLISECONDS);
      }

    } finally {
      alarmsLock.unlock();
    }
  }

  void acknowledgePendingMessages(boolean sync, Long offset) {
    Map<String, AckHandler> acksToSend = new HashMap<>();
    synchronized (pendingAcks) {
      if (!pendingAcks.isEmpty()) {
        for (Entry<String, AckHandler> pair : pendingAcks.entrySet()){
          acksToSend.put(pair.getKey(), pair.getValue());
        }
        logger.log(Level.FINER, "Sending {0} acks", acksToSend.size());
      }
    }

    if(offset != null) {
      acksToSend = filterAcksBeforeOffset(offset, acksToSend);
    }

    if(sync) {
      commitSync(acksToSend);
    } else {
      commitAsync(acksToSend);
    }
  }

  private Map<String, AckHandler> filterAcksBeforeOffset(Long offset, Map<String, AckHandler> acksToSend) {
    Map<String, AckHandler> filteredAcksToSend = new HashMap<>();
    for(Entry<String, AckHandler> ackToSend: acksToSend.entrySet()) {
      if(ackToSend.getValue().offset < offset) {
        filteredAcksToSend.put(ackToSend.getKey(), ackToSend.getValue());
      }
    }
    acksToSend = filteredAcksToSend;
    return acksToSend;
  }

  private void commitAsync(Map<String, AckHandler> acksToSend) {
    ListenableFuture<List<Empty>> listListenableFuture = ackProcessor
        .sendAckOperations(new ArrayList<>(acksToSend.keySet()), Collections.<PendingModifyAckDeadline>emptyList());

    Futures.addCallback(listListenableFuture, new FutureCallback<List<Empty>>() {
      @Override
      public void onSuccess(@Nullable List<Empty> empties) {
        handleSuccessfulAck(acksToSend);
      }

      @Override
      public void onFailure(Throwable throwable) {
        logger.log(Level.WARNING, "Failed to commit async", throwable);
      }
    });
  }

  private void commitSync(Map<String, AckHandler> acksToSend) {
    ListenableFuture<List<Empty>> listListenableFuture = ackProcessor
        .sendAckOperations(new ArrayList<>(acksToSend.keySet()), Collections.<PendingModifyAckDeadline>emptyList());

    try {
      listListenableFuture.get();
      handleSuccessfulAck(acksToSend);
    } catch (InterruptedException | ExecutionException e) {
      if(StatusUtil.isRetryable(e)) {
        sleep(this.retryBackoffMs);
        commitSync(acksToSend);
      } else {
        throw new KafkaException(e);
      }
    }
  }

  private void handleSuccessfulAck(
      Map<String, AckHandler> acksToSend) {
    synchronized (pendingAcks) {
      for(Entry<String, AckHandler> entry : acksToSend.entrySet()) {
        entry.getValue().acked.getAndSet(true);
        pendingAcks.remove(entry.getKey());
      }
    }
  }

  private void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }


  private void extendAckDeadlines(List<PendingModifyAckDeadline> ackDeadlineExtensions) {
    List<PendingModifyAckDeadline> modifyAckDeadlinesToSend =
        Lists.newArrayList(ackDeadlineExtensions);
    PendingModifyAckDeadline nacksToSend = new PendingModifyAckDeadline(0);
    synchronized (pendingNacks) {
      if (!pendingNacks.isEmpty()) {
        try {
          for (String ackId : pendingNacks) {
            nacksToSend.addAckId(ackId);
          }
          logger.log(Level.FINER, "Sending {0} nacks", pendingNacks.size());
        } finally {
          pendingNacks.clear();
        }
        modifyAckDeadlinesToSend.add(nacksToSend);
      }
    }
    ackProcessor.sendAckOperations(Collections.<String>emptyList(), modifyAckDeadlinesToSend);

  }
}