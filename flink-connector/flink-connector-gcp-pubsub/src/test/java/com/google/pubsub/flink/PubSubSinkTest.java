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
package com.google.pubsub.flink;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.base.Optional;
import com.google.pubsub.flink.internal.sink.PubSubFlushablePublisher;
import com.google.pubsub.flink.util.EmulatorEndpoint;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubSinkTest {
  @Test
  public void build_invalidTopic() throws Exception {
    assertThrows(NullPointerException.class, () -> PubSubSink.<String>builder().setTopicName(null));
    PubSubSink.Builder<String> builder =
        PubSubSink.<String>builder()
            .setProjectName("project")
            .setSerializationSchema(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()));
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  public void build_invalidProject() throws Exception {
    assertThrows(
        NullPointerException.class, () -> PubSubSink.<String>builder().setProjectName(null));
    PubSubSink.Builder<String> builder =
        PubSubSink.<String>builder()
            .setTopicName("topic")
            .setSerializationSchema(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()));
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  public void build_invalidSchema() throws Exception {
    assertThrows(
        NullPointerException.class,
        () -> PubSubSink.<String>builder().setSerializationSchema(null));
    PubSubSink.Builder<String> builder =
        PubSubSink.<String>builder().setProjectName("project").setTopicName("topic");
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  public void build_invalidCreds() throws Exception {
    assertThrows(
        NullPointerException.class, () -> PubSubSink.<String>builder().setCredentials(null));
  }

  @Test
  public void build_invalidEnableMessageOrdering() throws Exception {
    assertThrows(
        NullPointerException.class,
        () -> PubSubSink.<String>builder().setEnableMessageOrdering(null));
  }

  @Test
  public void build_invalidEndpoint() throws Exception {
    assertThrows(NullPointerException.class, () -> PubSubSink.<String>builder().setEndpoint(null));
  }

  @Test
  public void writersWithSameTopicAndDifferentConfigurationOwnSeparatePublishers()
      throws Exception {
    var first = spy(sinkBuilder().setEndpoint("emulator:///127.0.0.1:23456").build());
    var second =
        spy(
            sinkBuilder()
                .setEndpoint("emulator:///127.0.0.1:23457")
                .setEnableMessageOrdering(true)
                .build());
    Publisher firstPublisher = mock(Publisher.class);
    Publisher secondPublisher = mock(Publisher.class);
    doReturn(new PubSubFlushablePublisher(firstPublisher))
        .when(first)
        .createPublisher(TopicName.of("project", "topic"));
    doReturn(new PubSubFlushablePublisher(secondPublisher))
        .when(second)
        .createPublisher(TopicName.of("project", "topic"));
    when(firstPublisher.awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(true);
    when(secondPublisher.awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(true);
    var firstWriter = first.createWriter(mock(WriterInitContext.class));
    var secondWriter = second.createWriter(mock(WriterInitContext.class));
    try {
      firstWriter.close();
      verify(firstPublisher).shutdown();
      verify(secondPublisher, never()).shutdown();
      secondWriter.close();
      verify(secondPublisher).shutdown();
    } finally {
      firstWriter.close();
      secondWriter.close();
    }
  }

  @Test
  public void schemaOpenFailureDoesNotAllocatePublisher() throws Exception {
    PubSubSerializationSchema<String> schema = mock(PubSubSerializationSchema.class);
    IOException failure = new IOException("schema open failed");
    doThrow(failure).when(schema).open(any());
    var sink = spy(sinkBuilder().setSerializationSchema(schema).build());
    assertThat(
            assertThrows(IOException.class, () -> sink.createWriter(mock(WriterInitContext.class))))
        .hasCauseThat()
        .isSameInstanceAs(failure);
    verify(sink, never()).createPublisher(any(TopicName.class));
  }

  @Test
  public void transportCreationFailureClosesAllocatedExecutor() throws Exception {
    var sink = spy(sinkBuilder().setEndpoint("emulator:///127.0.0.1:23456").build());
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    ScheduledExecutorService callbacks = mock(ScheduledExecutorService.class);
    InstantiatingGrpcChannelProvider provider = mock(InstantiatingGrpcChannelProvider.class);
    IOException failure = new IOException("Transport creation failed");
    doReturn(executor).when(sink).createExecutor();
    doReturn(callbacks).when(sink).createCallbackExecutor();
    doReturn(provider).when(sink).createChannelProvider(any());
    when(provider.getTransportChannel()).thenThrow(failure);
    assertThat(
            assertThrows(IOException.class, () -> sink.createWriter(mock(WriterInitContext.class))))
        .isSameInstanceAs(failure);
    verify(executor).shutdownNow();
    verify(callbacks).shutdownNow();
  }

  @Test
  public void transportCreationFailurePreservesCauseWhenExecutorCleanupAlsoFails()
      throws Exception {
    var sink = spy(sinkBuilder().setEndpoint("emulator:///127.0.0.1:23456").build());
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    ScheduledExecutorService callbacks = mock(ScheduledExecutorService.class);
    InstantiatingGrpcChannelProvider provider = mock(InstantiatingGrpcChannelProvider.class);
    IOException failure = new IOException("Transport creation failed");
    RuntimeException callbackFailure = new RuntimeException("Callback executor cleanup failed");
    RuntimeException retryFailure = new RuntimeException("Retry executor cleanup failed");
    doReturn(executor).when(sink).createExecutor();
    doReturn(callbacks).when(sink).createCallbackExecutor();
    doReturn(provider).when(sink).createChannelProvider(any());
    when(provider.getTransportChannel()).thenThrow(failure);
    doThrow(callbackFailure).when(callbacks).shutdownNow();
    doThrow(retryFailure).when(executor).shutdownNow();

    IOException actual =
        assertThrows(IOException.class, () -> sink.createWriter(mock(WriterInitContext.class)));

    assertThat(actual).isSameInstanceAs(failure);
    assertThat(actual.getSuppressed())
        .asList()
        .containsExactly(callbackFailure, retryFailure)
        .inOrder();
    verify(executor).shutdownNow();
    verify(callbacks).shutdownNow();
  }

  @Test
  public void customEndpointWithCredentialsBuildsWriterAndUsesConfiguredTransport()
      throws Exception {
    String endpoint = "regional.pubsub.example:443";
    GoogleCredentials credentials =
        GoogleCredentials.create(
            new AccessToken("unit-test-token", Date.from(Instant.now().plusSeconds(3600))));
    var sink = spy(sinkBuilder().setEndpoint(endpoint).setCredentials(credentials).build());
    List<ManagedChannel> channels = new ArrayList<>();
    var provider =
        sink.createChannelProvider(null).toBuilder()
            .setPoolSize(1)
            .setChannelConfigurator(
                builder -> {
                  ManagedChannelBuilder<?> observed = spy(builder);
                  doAnswer(
                          invocation -> {
                            ManagedChannel channel = (ManagedChannel) invocation.callRealMethod();
                            channels.add(channel);
                            return channel;
                          })
                      .when(observed)
                      .build();
                  return observed;
                })
            .build();
    doReturn(provider).when(sink).createChannelProvider(isNull());
    // The integration profile exports an emulator host. Isolate this test's non-emulator path
    // without changing process-wide environment variables or disabling credentials on the SDK.
    try (var endpoints = mockStatic(EmulatorEndpoint.class)) {
      endpoints
          .when(() -> EmulatorEndpoint.getEmulatorEndpoint(Optional.of(endpoint)))
          .thenReturn(null);
      var writer = sink.createWriter(mock(WriterInitContext.class));
      try {
        assertThat(channels).isNotEmpty();
        for (ManagedChannel channel : channels) {
          assertThat(channel.authority()).isEqualTo(endpoint);
        }
      } finally {
        writer.close();
      }
      for (ManagedChannel channel : channels) {
        assertThat(channel.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
      }
    } finally {
      for (ManagedChannel channel : channels) {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      }
    }
  }

  private static PubSubSink.Builder<String> sinkBuilder() {
    return PubSubSink.<String>builder()
        .setProjectName("project")
        .setTopicName("topic")
        .setSerializationSchema(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()));
  }
}
