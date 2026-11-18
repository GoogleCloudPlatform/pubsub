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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.TransportChannel;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.base.Throwables;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.flink.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Publisher owned by one sink writer. Not thread-safe, and not usable after close. */
public class PubSubFlushablePublisher implements FlushablePublisher {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubFlushablePublisher.class);
  private static final long DEFAULT_CLOSE_TIMEOUT_SECONDS = 30;
  private final Publisher publisher;
  private final TransportChannel transport;
  private final ScheduledExecutorService executor;
  private final ScheduledExecutorService callbackExecutor;
  private final List<ApiFuture<String>> outstandingPublishes = new ArrayList<>();
  private final List<OwnedResource> owned = new ArrayList<>();
  private final long closeTimeoutNanos;
  private volatile boolean closed;

  public PubSubFlushablePublisher(Publisher publisher) {
    this(publisher, DEFAULT_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  public PubSubFlushablePublisher(
      Publisher publisher,
      TransportChannel transport,
      ScheduledExecutorService executor,
      ScheduledExecutorService callbackExecutor) {
    this(
        publisher,
        transport,
        executor,
        callbackExecutor,
        DEFAULT_CLOSE_TIMEOUT_SECONDS,
        TimeUnit.SECONDS);
  }

  PubSubFlushablePublisher(Publisher publisher, long closeTimeout, TimeUnit unit) {
    this(publisher, null, null, null, closeTimeout, unit);
  }

  PubSubFlushablePublisher(
      Publisher publisher,
      TransportChannel transport,
      ScheduledExecutorService executor,
      ScheduledExecutorService callbackExecutor,
      long closeTimeout,
      TimeUnit unit) {
    if (closeTimeout <= 0) {
      throw new IllegalArgumentException("Close timeout must be positive");
    }
    this.publisher = publisher;
    this.transport = transport;
    this.executor = executor;
    this.callbackExecutor = callbackExecutor;
    this.closeTimeoutNanos = unit.toNanos(closeTimeout);
    if (publisher != null) {
      owned.add(new OwnedResource("publisher", publisher::shutdown, publisher::awaitTermination));
    }
    if (transport != null) {
      owned.add(new OwnedResource("transport", transport::shutdown, transport::awaitTermination));
    }
    if (executor != null) {
      owned.add(
          new OwnedResource("publisher executor", executor::shutdown, executor::awaitTermination));
    }
    if (callbackExecutor != null) {
      owned.add(
          new OwnedResource(
              "callback executor", callbackExecutor::shutdown, callbackExecutor::awaitTermination));
    }
  }

  @Override
  public void publish(PubsubMessage message) throws InterruptedException {
    if (closed) {
      throw new IllegalStateException("Publisher is closed");
    }
    outstandingPublishes.add(publisher.publish(message));
  }

  @Override
  public void flush() throws IOException, InterruptedException {
    if (closed) {
      throw new IOException("Publisher is closed");
    }
    flushOutstanding();
  }

  private void flushOutstanding() throws IOException, InterruptedException {
    publisher.publishAllOutstanding();
    try {
      ApiFutures.allAsList(outstandingPublishes).get();
      outstandingPublishes.clear();
    } catch (ExecutionException failed) {
      throw new IOException("Pub/Sub publish failed", failed.getCause());
    } catch (CancellationException cancelled) {
      throw new IOException("Pub/Sub publish was cancelled", cancelled);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    long closeStarted = System.nanoTime();
    CompletableFuture<Void> cleanup = new CompletableFuture<>();
    Thread worker = startCleanupWorker(cleanup, closeStarted);
    try {
      cleanup.get(remainingNanos(closeStarted), TimeUnit.NANOSECONDS);
    } catch (TimeoutException timedOut) {
      throw abandon(
          worker,
          cleanup,
          closeStarted,
          new IOException(
              "Pub/Sub publisher close did not finish within " + closeTimeoutMillis() + " ms",
              timedOut));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw abandon(
          worker,
          cleanup,
          closeStarted,
          new IOException("Interrupted while closing Pub/Sub publisher", interrupted));
    } catch (ExecutionException failed) {
      Throwable cause = failed.getCause();
      forceResources(cause);
      Throwables.throwIfInstanceOf(cause, IOException.class);
      Throwables.throwIfInstanceOf(cause, Error.class);
      throw new IOException("Unable to close Pub/Sub publisher", cause);
    }
  }

  // Publisher.shutdown() ignores interrupts, so close waits on a worker with a deadline.
  private Thread startCleanupWorker(CompletableFuture<Void> cleanup, long closeStarted) {
    Thread worker =
        new Thread(
            () -> {
              try {
                closePublisher(closeStarted);
                cleanup.complete(null);
              } catch (Throwable failure) {
                cleanup.completeExceptionally(failure);
              }
            },
            "pubsub-publisher-close-" + publisher.getTopicNameString());
    worker.setDaemon(true);
    worker.start();
    return worker;
  }

  private long closeTimeoutMillis() {
    return TimeUnit.NANOSECONDS.toMillis(closeTimeoutNanos);
  }

  private void closePublisher(long closeStarted) throws IOException {
    IOException failure = null;
    try {
      flushOutstanding();
    } catch (IOException failed) {
      failure = failed;
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      failure = new IOException("Interrupted while flushing Pub/Sub publisher", interrupted);
    } catch (RuntimeException failed) {
      failure = new IOException("Unable to flush Pub/Sub publisher during close", failed);
    } finally {
      try {
        shutdownPublisher(closeStarted);
      } catch (IOException shutdownFailure) {
        failure = firstFailure(failure, shutdownFailure);
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  /**
   * Returns {@code first}, or {@code next} if nothing failed earlier; a later failure is attached
   * to it.
   */
  private static IOException firstFailure(IOException first, IOException next) {
    if (first == null) return next;
    first.addSuppressed(next);
    return first;
  }

  private void shutdownPublisher(long closeStarted) throws IOException {
    try {
      for (OwnedResource r : owned) r.shutdown().run();
      for (OwnedResource r : owned) awaitTermination(r.name(), r.await(), closeStarted);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while closing Pub/Sub publisher", interrupted);
    } catch (RuntimeException failure) {
      throw new IOException("Unable to shut down Pub/Sub publisher", failure);
    } finally {
      // Callbacks must outlive the publisher's shutdown wait, so this executor closes last.
      if (callbackExecutor != null) callbackExecutor.shutdownNow();
    }
  }

  private long remainingNanos(long closeStarted) {
    return Math.max(0, closeTimeoutNanos - (System.nanoTime() - closeStarted));
  }

  /** Stops waiting for the cleanup worker, aborts owned resources, and returns {@code failure}. */
  private IOException abandon(
      Thread worker, CompletableFuture<Void> cleanup, long closeStarted, IOException failure) {
    LOG.warn(
        "Publisher cleanup worker {} is still draining after {} ms; aborting owned resources",
        worker.getName(),
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - closeStarted));
    // Never cancel the future: the worker must still run its shutdown finally block.
    worker.interrupt();
    forceResources(failure);
    cleanup.whenComplete(
        (ignored, error) -> {
          if (error == null) {
            LOG.info(
                "Publisher cleanup worker {} finished after the caller stopped waiting",
                worker.getName());
          } else {
            LOG.warn(
                "Publisher cleanup worker {} failed after the caller stopped waiting",
                worker.getName(),
                error);
          }
        });
    return failure;
  }

  private void forceResources(Throwable failure) {
    // The callback executor is left to the worker; see shutdownPublisher.
    forceCloseResources(failure, transport, executor);
  }

  /** Force-closes the given resources; any cleanup errors are attached to {@code failure}. */
  @Internal
  public static void forceCloseResources(
      Throwable failure, TransportChannel transport, ExecutorService... executors) {
    Error fatal = null;
    if (transport != null) fatal = shutdownNow(failure, fatal, transport::shutdownNow);
    for (ExecutorService executor : executors) {
      if (executor != null) fatal = shutdownNow(failure, fatal, executor::shutdownNow);
    }
    if (fatal != null) throw fatal;
  }

  /** Runs one cleanup step; records exceptions on {@code failure} and returns the first Error. */
  private static Error shutdownNow(Throwable failure, Error fatal, Runnable cleanup) {
    try {
      cleanup.run();
    } catch (RuntimeException cleanupFailure) {
      if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
    } catch (Error error) {
      if (fatal == null) return error;
      fatal.addSuppressed(error);
    }
    return fatal;
  }

  private interface Terminable {
    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;
  }

  private record OwnedResource(String name, Runnable shutdown, Terminable await) {}

  private void awaitTermination(String name, Terminable resource, long closeStarted)
      throws IOException, InterruptedException {
    if (!resource.awaitTermination(remainingNanos(closeStarted), TimeUnit.NANOSECONDS)) {
      throw new IOException("Pub/Sub " + name + " did not terminate within the close budget");
    }
  }
}
