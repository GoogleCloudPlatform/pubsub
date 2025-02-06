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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.auth.Credentials;
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.common.base.Optional;
import com.google.pubsub.flink.internal.sink.PubSubFlushablePublisher;
import com.google.pubsub.flink.internal.sink.PubSubPublisherCache;
import com.google.pubsub.flink.internal.sink.PubSubSinkWriter;
import com.google.pubsub.flink.util.EmulatorEndpoint;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

/**
 * Google Cloud Pub/Sub sink to publish messages to a Pub/Sub topic.
 *
 * <p>{@link PubSubSink} is constructed and configured using {@link Builder}. {@link PubSubSink}
 * cannot be configured after it is built. See {@link Builder} for how {@link PubSubSink} can be
 * configured.
 */
@AutoValue
public abstract class PubSubSink<T> implements Sink<T> {
  public abstract String projectName();

  public abstract String topicName();

  public abstract PubSubSerializationSchema<T> serializationSchema();

  public abstract Optional<Credentials> credentials();

  public abstract Optional<Boolean> enableMessageOrdering();

  public abstract Optional<String> endpoint();

  public static <T> Builder<T> builder() {
    return new AutoValue_PubSubSink.Builder<T>();
  }

  private Publisher createPublisher(TopicName topicName) throws IOException {
    Publisher.Builder builder = Publisher.newBuilder(topicName.toString());
    // Channel settings copied from com.google.cloud:google-cloud-pubsub:1.124.1.
    builder.setChannelProvider(
        TopicAdminSettings.defaultGrpcTransportProviderBuilder()
            .setChannelsPerCpu(1)
            .setHeaderProvider(
                FixedHeaderProvider.create(
                    "x-goog-api-client", "PubSub-Flink-Connector/1.0.0-SNAPSHOT"))
            .build());
    if (credentials().isPresent()) {
      builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials().get()));
    }
    if (enableMessageOrdering().isPresent()) {
      builder.setEnableMessageOrdering(enableMessageOrdering().get());
    }
    if (endpoint().isPresent()) {
      builder.setEndpoint(endpoint().get());
    }

    String emulatorEndpoint = EmulatorEndpoint.getEmulatorEndpoint(endpoint());
    if (emulatorEndpoint != null) {
      builder.setCredentialsProvider(NoCredentialsProvider.create());
      builder.setChannelProvider(
          FixedTransportChannelProvider.create(
              GrpcTransportChannel.create(
                  ManagedChannelBuilder.forTarget(emulatorEndpoint).usePlaintext().build())));
    }
    return builder.build();
  }

  @Override
  public SinkWriter<T> createWriter(InitContext initContext) throws IOException {
    try {
      serializationSchema().open(initContext.asSerializationSchemaInitializationContext());
    } catch (Exception e) {
      throw new IOException(e);
    }
    return new PubSubSinkWriter<>(
        new PubSubFlushablePublisher(
            PubSubPublisherCache.getOrCreate(
                TopicName.of(projectName(), topicName()), this::createPublisher)),
        serializationSchema());
  }

  /** Builder to construct {@link PubSubSink}. */
  @AutoValue.Builder
  public abstract static class Builder<T> {
    /**
     * Sets the GCP project ID that owns the topic to which messages are published.
     *
     * <p>Setting this option is required to build {@link PubSubSink}.
     */
    public abstract Builder<T> setProjectName(String projectName);

    /**
     * Sets the Pub/Sub topic to which messages are published.
     *
     * <p>Setting this option is required to build {@link PubSubSink}.
     */
    public abstract Builder<T> setTopicName(String topicName);

    /**
     * Sets the serialization schema used to serialize incoming data into a {@link PubsubMessage}.
     *
     * <p>Setting this option is required to build {@link PubSubSink}.
     */
    public abstract Builder<T> setSerializationSchema(
        PubSubSerializationSchema<T> serializationSchema);

    /**
     * Sets the credentials used when publishing messages to Google Cloud Pub/Sub.
     *
     * <p>If not set, then Application Default Credentials are used for authentication.
     */
    public abstract Builder<T> setCredentials(Credentials credentials);

    /**
     * Sets whether to enable ordered publishing.
     *
     * <p>The default value is {@code false}. This must be set to {@code true} when publishing an
     * {@link PubsubMessage} with an ordering key set.
     */
    public abstract Builder<T> setEnableMessageOrdering(Boolean enableMessageOrdering);

    /**
     * Sets the Google Cloud Pub/Sub service endpoint to which messages are published.
     *
     * <p>Defaults to connecting to the global endpoint, which routes requests to the nearest
     * regional endpoint.
     */
    public abstract Builder<T> setEndpoint(String endpoint);

    public abstract PubSubSink<T> build();
  }
}
