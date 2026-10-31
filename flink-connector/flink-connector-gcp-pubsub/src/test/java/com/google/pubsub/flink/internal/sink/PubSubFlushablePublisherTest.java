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
package com.google.pubsub.flink.internal.sink;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.TransportChannel;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubFlushablePublisherTest {
  @Mock Publisher mockPublisher;
  PubSubFlushablePublisher flushablePublisher;

  @Before
  public void doBeforeEachTest() {
    flushablePublisher = new PubSubFlushablePublisher(mockPublisher);
  }

  @Test
  public void close_shutdownIgnoringInterruptsDoesNotBlockCallerOrStartAnotherWorker()
      throws Exception {
    CountDownLatch shutdownEntered = new CountDownLatch(1);
    CountDownLatch releaseShutdown = new CountDownLatch(1);
    AtomicReference<Thread> shutdownThread = new AtomicReference<>();
    doAnswer(
            invocation -> {
              shutdownThread.set(Thread.currentThread());
              shutdownEntered.countDown();
              Uninterruptibles.awaitUninterruptibly(releaseShutdown);
              return null;
            })
        .when(mockPublisher)
        .shutdown();
    flushablePublisher = new PubSubFlushablePublisher(mockPublisher, 100, TimeUnit.MILLISECONDS);
    var caller = Executors.newSingleThreadExecutor();
    try {
      Future<IOException> close =
          caller.submit(() -> assertThrows(IOException.class, flushablePublisher::close));
      assertThat(shutdownEntered.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(close.get(2, TimeUnit.SECONDS))
          .hasCauseThat()
          .isInstanceOf(TimeoutException.class);
      assertThat(shutdownThread.get().isDaemon()).isTrue();
      caller
          .submit(
              () -> {
                flushablePublisher.close();
                return null;
              })
          .get(2, TimeUnit.SECONDS);
      verify(mockPublisher, times(1)).shutdown();
      assertThrows(
          IllegalStateException.class,
          () -> flushablePublisher.publish(PubsubMessage.getDefaultInstance()));
    } finally {
      releaseShutdown.countDown();
      caller.shutdownNow();
      assertThat(caller.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
      if (shutdownThread.get() != null) {
        shutdownThread.get().join(5000);
        assertThat(shutdownThread.get().isAlive()).isFalse();
      }
    }
  }

  @Test
  public void close_unfinishedPublishIsBoundedAndStillAttemptsShutdown() throws Exception {
    PubsubMessage message = PubsubMessage.getDefaultInstance();
    SettableApiFuture<String> publish = SettableApiFuture.create();
    when(mockPublisher.publish(message)).thenReturn(publish);
    CountDownLatch shutdownEntered = new CountDownLatch(1);
    AtomicReference<Thread> shutdownThread = new AtomicReference<>();
    doAnswer(
            invocation -> {
              shutdownThread.set(Thread.currentThread());
              shutdownEntered.countDown();
              return null;
            })
        .when(mockPublisher)
        .shutdown();
    flushablePublisher = new PubSubFlushablePublisher(mockPublisher, 100, TimeUnit.MILLISECONDS);
    flushablePublisher.publish(message);
    var caller = Executors.newSingleThreadExecutor();
    try {
      Future<IOException> close =
          caller.submit(() -> assertThrows(IOException.class, flushablePublisher::close));
      assertThat(close.get(2, TimeUnit.SECONDS))
          .hasCauseThat()
          .isInstanceOf(TimeoutException.class);
      assertThat(shutdownEntered.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(publish.isCancelled()).isFalse();
    } finally {
      publish.set("ack-id");
      caller.shutdownNow();
      assertThat(caller.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
      assertThat(shutdownEntered.await(5, TimeUnit.SECONDS)).isTrue();
      shutdownThread.get().join(5000);
      assertThat(shutdownThread.get().isAlive()).isFalse();
    }
  }

  @Test
  public void close_callerInterruptionDoesNotWaitForUninterruptibleShutdown() throws Exception {
    CountDownLatch shutdownEntered = new CountDownLatch(1);
    CountDownLatch releaseShutdown = new CountDownLatch(1);
    AtomicReference<Thread> callerThread = new AtomicReference<>();
    AtomicReference<Thread> shutdownThread = new AtomicReference<>();
    doAnswer(
            invocation -> {
              shutdownThread.set(Thread.currentThread());
              shutdownEntered.countDown();
              Uninterruptibles.awaitUninterruptibly(releaseShutdown);
              return null;
            })
        .when(mockPublisher)
        .shutdown();
    var caller = Executors.newSingleThreadExecutor();
    try {
      Future<IOException> close =
          caller.submit(
              () -> {
                callerThread.set(Thread.currentThread());
                IOException failure = assertThrows(IOException.class, flushablePublisher::close);
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
                return failure;
              });
      assertThat(shutdownEntered.await(5, TimeUnit.SECONDS)).isTrue();
      callerThread.get().interrupt();
      assertThat(close.get(2, TimeUnit.SECONDS))
          .hasCauseThat()
          .isInstanceOf(InterruptedException.class);
    } finally {
      releaseShutdown.countDown();
      caller.shutdownNow();
      assertThat(caller.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
      if (shutdownThread.get() != null) {
        shutdownThread.get().join(5000);
        assertThat(shutdownThread.get().isAlive()).isFalse();
      }
    }
  }

  @Test
  public void constructor_rejectsNonpositiveCloseTimeout() {
    for (long timeout : new long[] {0, -1}) {
      assertThrows(
          IllegalArgumentException.class,
          () -> new PubSubFlushablePublisher(mockPublisher, timeout, TimeUnit.SECONDS));
    }
  }

  @Test
  public void close_terminationWaitUsesRemainingConfiguredBudget() throws Exception {
    flushablePublisher = new PubSubFlushablePublisher(mockPublisher, 1, TimeUnit.SECONDS);
    when(mockPublisher.awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS)))
        .thenAnswer(
            invocation -> {
              long remaining = invocation.getArgument(0);
              assertThat(remaining).isGreaterThan(0L);
              assertThat(remaining).isAtMost(TimeUnit.SECONDS.toNanos(1));
              return true;
            });
    flushablePublisher.close();
  }

  @Test
  public void close_shutdownFailureStillForcesBothOwnedResources() {
    TransportChannel transport = mock(TransportChannel.class);
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    RuntimeException sdkFailure = new RuntimeException("SDK shutdown failed");
    RuntimeException transportFailure = new RuntimeException("Transport abort failed");
    doThrow(sdkFailure).when(mockPublisher).shutdown();
    doThrow(transportFailure).when(transport).shutdownNow();
    flushablePublisher = new PubSubFlushablePublisher(mockPublisher, transport, executor, null);
    IOException failure = assertThrows(IOException.class, flushablePublisher::close);
    assertThat(failure).hasCauseThat().isSameInstanceAs(sdkFailure);
    assertThat(failure.getSuppressed()).asList().containsExactly(transportFailure);
    verify(executor).shutdownNow();
  }

  @Test
  public void publish_success() throws Exception {
    PubsubMessage message =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message")).build();
    when(mockPublisher.publish(message)).thenReturn(ApiFutures.immediateFuture("ack-id"));

    flushablePublisher.publish(message);
    verify(mockPublisher).publish(message);

    flushablePublisher.flush();
  }

  @Test
  public void publish_singleFailure() throws Exception {
    PubsubMessage message1 =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message1")).build();
    when(mockPublisher.publish(message1)).thenReturn(ApiFutures.immediateFuture("ack-id"));
    PubsubMessage message2 =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message2")).build();
    when(mockPublisher.publish(message2))
        .thenReturn(ApiFutures.immediateFailedFuture(new RuntimeException()));

    flushablePublisher.publish(message1);
    verify(mockPublisher).publish(message1);
    flushablePublisher.publish(message2);
    verify(mockPublisher).publish(message2);

    assertThrows(IOException.class, flushablePublisher::flush);
  }

  @Test
  public void flush_blocksUntilPublishesComplete() throws Exception {
    PubsubMessage message =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message")).build();
    SettableApiFuture<String> messageFuture = SettableApiFuture.create();
    when(mockPublisher.publish(message)).thenReturn(messageFuture);

    flushablePublisher.publish(message);
    verify(mockPublisher).publish(message);

    var executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> flushFuture =
          executor.submit(
              () -> {
                flushablePublisher.flush();
                return null;
              });
      assertThrows(TimeoutException.class, () -> flushFuture.get(200, TimeUnit.MILLISECONDS));
      messageFuture.set("ack-id");
      flushFuture.get(5, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void flush_propagatesInterruptionAndDoesNotForgetOutstandingPublish() throws Exception {
    PubsubMessage message = PubsubMessage.getDefaultInstance();
    var result = SettableApiFuture.<String>create();
    when(mockPublisher.publish(message)).thenReturn(result);
    flushablePublisher.publish(message);
    try {
      Thread.currentThread().interrupt();
      assertThrows(InterruptedException.class, flushablePublisher::flush);
    } finally {
      Thread.interrupted();
    }
    RuntimeException publishFailure = new RuntimeException("rejected publication");
    result.setException(publishFailure);
    IOException failure = assertThrows(IOException.class, flushablePublisher::flush);
    assertThat(failure).hasCauseThat().isSameInstanceAs(publishFailure);
  }

  @Test
  public void flush_asyncFailureRemainsVisibleOnSubsequentFlush() throws Exception {
    PubsubMessage message = PubsubMessage.getDefaultInstance();
    RuntimeException failure = new RuntimeException("rejected publication");
    when(mockPublisher.publish(message)).thenReturn(ApiFutures.immediateFailedFuture(failure));
    flushablePublisher.publish(message);
    assertThat(assertThrows(IOException.class, flushablePublisher::flush))
        .hasCauseThat()
        .isSameInstanceAs(failure);
    assertThat(assertThrows(IOException.class, flushablePublisher::flush))
        .hasCauseThat()
        .isSameInstanceAs(failure);
  }

  @Test
  public void close_flushesShutsDownAndWaitsOnce() throws Exception {
    when(mockPublisher.awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(true);
    flushablePublisher.close();
    flushablePublisher.close();
    verify(mockPublisher).publishAllOutstanding();
    verify(mockPublisher).shutdown();
    verify(mockPublisher).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
    assertThrows(
        IllegalStateException.class,
        () -> flushablePublisher.publish(PubsubMessage.getDefaultInstance()));
    assertThrows(IOException.class, flushablePublisher::flush);
  }

  @Test
  public void close_flushFailureStillShutsDownAndPreservesProviderCause() throws Exception {
    PubsubMessage message = PubsubMessage.getDefaultInstance();
    RuntimeException failure = new RuntimeException("rejected publication");
    when(mockPublisher.publish(message)).thenReturn(ApiFutures.immediateFailedFuture(failure));
    when(mockPublisher.awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(true);
    flushablePublisher.publish(message);
    assertThat(assertThrows(IOException.class, flushablePublisher::close))
        .hasCauseThat()
        .isSameInstanceAs(failure);
    verify(mockPublisher).shutdown();
    verify(mockPublisher).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
  }

  @Test
  public void close_keepsFlushFailureWhenShutdownAlsoFails() throws Exception {
    PubsubMessage message = PubsubMessage.getDefaultInstance();
    RuntimeException publishFailure = new RuntimeException("rejected publication");
    RuntimeException shutdownFailure = new RuntimeException("shutdown failure");
    when(mockPublisher.publish(message))
        .thenReturn(ApiFutures.immediateFailedFuture(publishFailure));
    doThrow(shutdownFailure).when(mockPublisher).shutdown();
    flushablePublisher.publish(message);
    IOException failure = assertThrows(IOException.class, flushablePublisher::close);
    assertThat(failure).hasCauseThat().isSameInstanceAs(publishFailure);
    assertThat(failure.getSuppressed()).hasLength(1);
    assertThat(failure.getSuppressed()[0]).hasCauseThat().isSameInstanceAs(shutdownFailure);
  }

  @Test
  public void close_workerInterruptionDoesNotInterruptCaller() throws Exception {
    when(mockPublisher.awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS)))
        .thenThrow(new InterruptedException("shutdown interrupted"));
    try {
      IOException failure = assertThrows(IOException.class, flushablePublisher::close);
      assertThat(failure).hasCauseThat().isInstanceOf(InterruptedException.class);
      assertThat(Thread.currentThread().isInterrupted()).isFalse();
      verify(mockPublisher).shutdown();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  public void close_interruptedCallerStillStartsShutdown() throws Exception {
    PubsubMessage message = PubsubMessage.getDefaultInstance();
    when(mockPublisher.publish(message)).thenReturn(SettableApiFuture.create());
    CountDownLatch terminated = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              terminated.countDown();
              return true;
            })
        .when(mockPublisher)
        .awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
    flushablePublisher.publish(message);
    try {
      Thread.currentThread().interrupt();
      IOException failure = assertThrows(IOException.class, flushablePublisher::close);
      assertThat(failure).hasCauseThat().isInstanceOf(InterruptedException.class);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
    assertThat(terminated.await(5, TimeUnit.SECONDS)).isTrue();
    verify(mockPublisher).shutdown();
    verify(mockPublisher).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
  }

  @Test
  public void close_terminationTimeoutIsReported() throws Exception {
    IOException failure = assertThrows(IOException.class, flushablePublisher::close);
    assertThat(failure).hasMessageThat().contains("did not terminate");
    verify(mockPublisher).shutdown();
    verify(mockPublisher).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
  }
}
