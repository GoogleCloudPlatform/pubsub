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

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies writer-owned publisher transport and executor closure against an isolated local emulator
 * topic.
 */
public class PubSubSinkChannelEmulatorIT {
  private final HttpClient http = HttpClient.newHttpClient();
  private final List<ManagedChannel> channels = new ArrayList<>();
  private final List<ScheduledExecutorService> executors = new ArrayList<>();
  private String endpoint;
  private String topic;
  private SinkWriter<String> writer;

  @Before
  public void createTopic() throws Exception {
    endpoint = PubSubEmulator.endpoint();
    topic = "projects/sink-channel-repro/topics/publisher-" + UUID.randomUUID();
    api("PUT", topic, "{}");
  }

  @Test(timeout = 30000)
  public void close_afterAcceptedPublishTerminatesSdkOwnedChannel() throws Exception {
    PubSubSink<String> sink =
        spy(
            PubSubSink.<String>builder()
                .setProjectName("sink-channel-repro")
                .setTopicName(topic.substring(topic.lastIndexOf('/') + 1))
                .setEndpoint("emulator:///" + endpoint)
                .setSerializationSchema(
                    PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
                .build());
    doAnswer(
            invocation -> {
              ScheduledExecutorService executor =
                  (ScheduledExecutorService) invocation.callRealMethod();
              executors.add(executor);
              return executor;
            })
        .when(sink)
        .createExecutor();
    doAnswer(
            invocation -> {
              ScheduledExecutorService executor =
                  (ScheduledExecutorService) invocation.callRealMethod();
              executors.add(executor);
              return executor;
            })
        .when(sink)
        .createCallbackExecutor();
    doAnswer(
            invocation -> {
              InstantiatingGrpcChannelProvider provider =
                  (InstantiatingGrpcChannelProvider) invocation.callRealMethod();
              var plaintext = provider.toBuilder().getChannelConfigurator();
              return provider.toBuilder()
                  .setChannelConfigurator(
                      builder -> {
                        ManagedChannelBuilder<?> observed = spy(plaintext.apply(builder));
                        doAnswer(
                                build -> {
                                  ManagedChannel created = (ManagedChannel) build.callRealMethod();
                                  channels.add(created);
                                  return created;
                                })
                            .when(observed)
                            .build();
                        return observed;
                      })
                  .build();
            })
        .when(sink)
        .createChannelProvider(any());
    writer = sink.createWriter(mock(WriterInitContext.class));
    writer.write("sink-record", null);
    writer.flush(false); // Waits for the emulator to accept the publish, not just enqueue it.
    assertTrue("Publisher must allocate a transport", !channels.isEmpty());
    for (ManagedChannel channel : channels) {
      assertTrue("Publisher transport must remain open until close", !channel.isShutdown());
    }
    writer.close();
    for (ScheduledExecutorService executor : executors) {
      assertTrue(
          "Sink close must terminate its publisher executor",
          executor.awaitTermination(5, TimeUnit.SECONDS));
    }
    for (ManagedChannel channel : channels) {
      assertTrue(
          "Sink close must terminate its publisher transport",
          channel.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @After
  public void cleanUp() throws Exception {
    try {
      if (writer != null) {
        writer.close();
      }
    } finally {
      for (ManagedChannel channel : channels) {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      }
      for (ScheduledExecutorService executor : executors) {
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
      }
      if (topic != null) {
        api("DELETE", topic, "");
      }
    }
  }

  private void api(String method, String resource, String body) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create("http://" + endpoint + "/v1/" + resource))
            .timeout(Duration.ofSeconds(5))
            .header("Content-Type", "application/json")
            .method(method, HttpRequest.BodyPublishers.ofString(body))
            .build();
    HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
    assertTrue(response.body(), response.statusCode() >= 200 && response.statusCode() < 300);
  }
}
