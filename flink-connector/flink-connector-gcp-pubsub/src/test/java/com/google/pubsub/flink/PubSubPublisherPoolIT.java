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
package com.google.pubsub.flink;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.flink.internal.sink.PubSubPublisherExecutor;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.junit.Test;

/**
 * Local throughput comparison; speed is reported, while correctness and pool bounds are asserted.
 */
public class PubSubPublisherPoolIT {
  @Test(timeout = 120000)
  public void concurrentPublishing_deliversEveryRecordWithBoundedPools() throws Exception {
    AtomicLong accepted = new AtomicLong();
    Server server =
        ServerBuilder.forPort(0)
            .directExecutor()
            .addService(
                new PublisherGrpc.PublisherImplBase() {
                  @Override
                  public void publish(
                      PublishRequest request, StreamObserver<PublishResponse> reply) {
                    PublishResponse.Builder response = PublishResponse.newBuilder();
                    for (int i = 0; i < request.getMessagesCount(); i++) {
                      response.addMessageIds(Long.toString(accepted.incrementAndGet()));
                    }
                    reply.onNext(response.build());
                    reply.onCompleted();
                  }
                })
            .build()
            .start();
    try {
      int baseline =
          TopicAdminSettings.defaultExecutorProviderBuilder().build().getExecutorThreadCount();
      for (int[] budget :
          new int[][] {
            {baseline, baseline}, {2, 2}, {4, 2}, {4, 2}, {2, 2}, {baseline, baseline}
          }) {
        exercise(server.getPort(), accepted, budget[0], budget[1]);
      }
    } finally {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private void exercise(int port, AtomicLong accepted, int publishingThreads, int callbackThreads)
      throws Exception {
    int writers = 4;
    int warmup = 500;
    int records = 10000;
    long before = accepted.get();
    var tasks = Executors.newFixedThreadPool(writers);
    List<ScheduledThreadPoolExecutor> pools = new ArrayList<>();
    List<Future<?>> futures = new ArrayList<>();
    CountDownLatch ready = new CountDownLatch(writers);
    CountDownLatch start = new CountDownLatch(1);
    try {
      for (int writer = 0; writer < writers; writer++) {
        var sink =
            spy(
                PubSubSink.<String>builder()
                    .setProjectName("pool-test")
                    .setTopicName("input")
                    .setEndpoint("emulator:///127.0.0.1:" + port)
                    .setSerializationSchema(
                        PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
                    .build());
        ScheduledThreadPoolExecutor publishing =
            publishingThreads == 2
                ? (ScheduledThreadPoolExecutor) sink.createExecutor()
                : new PubSubPublisherExecutor(publishingThreads, Executors.defaultThreadFactory());
        ScheduledThreadPoolExecutor callbacks =
            callbackThreads == 2
                ? (ScheduledThreadPoolExecutor) sink.createCallbackExecutor()
                : new ScheduledThreadPoolExecutor(callbackThreads);
        pools.add(publishing);
        pools.add(callbacks);
        // Keep this local server selected even when the surrounding profile exports an emulator.
        doReturn(sink.createChannelProvider("127.0.0.1:" + port))
            .when(sink)
            .createChannelProvider(any());
        doReturn(publishing).when(sink).createExecutor();
        doReturn(callbacks).when(sink).createCallbackExecutor();
        futures.add(
            tasks.submit(
                () -> {
                  var output = sink.createWriter(mock(WriterInitContext.class));
                  try {
                    for (int i = 0; i < warmup; i++) output.write("warmup", null);
                    output.flush(false);
                    ready.countDown();
                    assertThat(start.await(20, TimeUnit.SECONDS)).isTrue();
                    for (int i = 0; i < records; i++)
                      output.write("payload-" + "x".repeat(256), null);
                    output.flush(false);
                  } finally {
                    output.close();
                  }
                  return null;
                }));
      }
      assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
      long started = System.nanoTime();
      start.countDown();
      for (Future<?> future : futures) future.get(30, TimeUnit.SECONDS);
      long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
      assertThat(accepted.get() - before).isEqualTo((long) writers * (warmup + records));
      for (int i = 0; i < pools.size(); i++) {
        assertThat(pools.get(i).getLargestPoolSize())
            .isAtMost(i % 2 == 0 ? publishingThreads : callbackThreads);
        assertThat(pools.get(i).awaitTermination(5, TimeUnit.SECONDS)).isTrue();
      }
      System.out.printf(
          "PUBLISHER_POOL writers=%d records=%d publisherThreads=%d callbackThreads=%d millis=%d"
              + " recordsPerSecond=%.0f%n",
          writers,
          writers * records,
          publishingThreads,
          callbackThreads,
          millis,
          (double) writers * records * 1000 / Math.max(1, millis));
    } finally {
      start.countDown();
      tasks.shutdownNow();
      for (var pool : pools) pool.shutdownNow();
      assertThat(tasks.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }
  }
}
