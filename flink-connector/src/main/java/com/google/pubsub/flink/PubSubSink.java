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
import com.google.auth.Credentials;
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.flink.internal.sink.PubSubFlushablePublisher;
import com.google.pubsub.flink.internal.sink.PubSubPublisherCache;
import com.google.pubsub.flink.internal.sink.PubSubSinkWriter;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.Optional;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

@AutoValue
public abstract class PubSubSink<T> implements Sink<T> {
  public abstract String projectName();

  public abstract String topicName();

  public abstract PubSubSerializationSchema<T> serializationSchema();

  public abstract Optional<Credentials> credentials();

  public static <T> Builder<T> builder() {
    return new AutoValue_PubSubSink.Builder<T>();
  }

  private Publisher createPublisher(TopicName topicName) throws IOException {
    Publisher.Builder builder = Publisher.newBuilder(topicName.toString());
    if (credentials().isPresent()) {
      builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials().get()));
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

  @AutoValue.Builder
  public abstract static class Builder<T> {
    public abstract Builder<T> setProjectName(String projectName);

    public abstract Builder<T> setTopicName(String topicName);

    public abstract Builder<T> setSerializationSchema(
        PubSubSerializationSchema<T> serializationSchema);

    public abstract Builder<T> setCredentials(Credentials credentials);

    public abstract PubSubSink<T> build();
  }
}
