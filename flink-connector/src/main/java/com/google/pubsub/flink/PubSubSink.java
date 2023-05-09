/*
 * Copyright 2021 Google LLC
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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.flink.internal.sink.PubSubFlushablePublisher;
import com.google.pubsub.flink.internal.sink.PubSubPublisherCache;
import com.google.pubsub.flink.internal.sink.PubSubSinkWriter;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.Preconditions;

public class PubSubSink<T> implements Sink<T> {
  private final TopicName topicName;
  private final CredentialsProvider credentialsProvider;
  private final PubSubSerializationSchema serializationSchema;

  private PubSubSink(PubSubSinkBuilder builder) {
    this.topicName = TopicName.of(Preconditions.checkNotNull(builder.projectName), Preconditions.checkNotNull(builder.topicName));
    this.credentialsProvider = Preconditions.checkNotNull(builder.credentialsProvider);
    this.serializationSchema = Preconditions.checkNotNull(builder.serializationSchema);
  }

  public static <T> PubSubSinkBuilder<T> builder() {
    return new PubSubSinkBuilder<>();
  }

  private Publisher createPublisher() throws IOException {
    return Publisher.newBuilder(topicName.toString())
        .setCredentialsProvider(credentialsProvider)
        .build();
  }

  @Override
  public SinkWriter<T> createWriter(InitContext initContext) throws IOException {
    return new PubSubSinkWriter<>(
        new PubSubFlushablePublisher(PubSubPublisherCache.getOrCreate(topicName, this::createPublisher)),
        serializationSchema);
  }

  public static final class PubSubSinkBuilder<T> {
    PubSubSerializationSchema<T> serializationSchema;
    String projectName;
    String topicName;
    CredentialsProvider credentialsProvider =
        TopicAdminSettings.defaultCredentialsProviderBuilder().build();

    public PubSubSinkBuilder<T> withCredentials(Credentials credentials) {
      this.credentialsProvider =
          FixedCredentialsProvider.create(Preconditions.checkNotNull(credentials));
      return this;
    }

    public PubSubSinkBuilder<T> withProjectName(String projectName) {
      this.projectName = Preconditions.checkNotNull(projectName);
      return this;
    }

    public PubSubSinkBuilder<T> withTopicName(String topicName) {
      this.topicName = Preconditions.checkNotNull(topicName);
      return this;
    }

    public PubSubSinkBuilder<T> withSerializationSchema(PubSubSerializationSchema schema) {
      this.serializationSchema = Preconditions.checkNotNull(schema);
      return this;
    }

    public PubSubSink<T> build() {
      return new PubSubSink<>(this);
    }
  }

}
