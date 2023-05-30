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
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.flink.internal.source.enumerator.PubSubCheckpointSerializer;
import com.google.pubsub.flink.internal.source.enumerator.PubSubSplitEnumerator;
import com.google.pubsub.flink.internal.source.reader.AckTracker;
import com.google.pubsub.flink.internal.source.reader.PubSubNotifyingPullSubscriber;
import com.google.pubsub.flink.internal.source.reader.PubSubRecordEmitter;
import com.google.pubsub.flink.internal.source.reader.PubSubSourceReader;
import com.google.pubsub.flink.internal.source.reader.PubSubSplitReader;
import com.google.pubsub.flink.internal.source.split.SubscriptionSplit;
import com.google.pubsub.flink.internal.source.split.SubscriptionSplitSerializer;
import com.google.pubsub.flink.proto.PubSubEnumeratorCheckpoint;
import com.google.pubsub.v1.ProjectSubscriptionName;
import java.util.HashMap;
import java.util.Optional;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

@AutoValue
public abstract class PubSubSource<OutputT>
    implements Source<OutputT, SubscriptionSplit, PubSubEnumeratorCheckpoint>,
        ResultTypeQueryable<OutputT> {
  public abstract String projectName();

  public abstract String subscriptionName();

  public abstract PubSubDeserializationSchema<OutputT> deserializationSchema();

  public abstract Optional<Credentials> credentials();

  public static <OutputT> Builder<OutputT> builder() {
    return new AutoValue_PubSubSource.Builder<OutputT>();
  }

  Subscriber createSubscriber(MessageReceiver receiver) {
    Subscriber.Builder builder =
        Subscriber.newBuilder(
            ProjectSubscriptionName.of(projectName(), subscriptionName()).toString(), receiver);
    if (credentials().isPresent()) {
      builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials().get()));
    }
    return builder.build();
  }

  private PubSubSplitReader createSplitReader(AckTracker ackTracker) {
    return new PubSubSplitReader(
        () -> new PubSubNotifyingPullSubscriber(this::createSubscriber, ackTracker));
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<OutputT, SubscriptionSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    PubSubDeserializationSchema<OutputT> schema = deserializationSchema();
    schema.open(
        new DeserializationSchema.InitializationContext() {
          @Override
          public MetricGroup getMetricGroup() {
            return readerContext.metricGroup();
          }

          @Override
          public UserCodeClassLoader getUserCodeClassLoader() {
            return null;
          }
        });
    return new PubSubSourceReader<>(
        new PubSubRecordEmitter<>(schema),
        this::createSplitReader,
        new Configuration(),
        readerContext);
  }

  @Override
  public SplitEnumerator<SubscriptionSplit, PubSubEnumeratorCheckpoint> createEnumerator(
      SplitEnumeratorContext<SubscriptionSplit> enumContext) {
    return new PubSubSplitEnumerator(
        ProjectSubscriptionName.of(projectName(), subscriptionName()),
        enumContext,
        new HashMap<Integer, SubscriptionSplit>());
  }

  @Override
  public SplitEnumerator<SubscriptionSplit, PubSubEnumeratorCheckpoint> restoreEnumerator(
      SplitEnumeratorContext<SubscriptionSplit> enumContext,
      PubSubEnumeratorCheckpoint checkpoint) {
    HashMap<Integer, SubscriptionSplit> assignments = new HashMap<>();
    for (PubSubEnumeratorCheckpoint.Assignment assignment : checkpoint.getAssignmentsList()) {
      assignments.put(assignment.getSubtask(), SubscriptionSplit.fromProto(assignment.getSplit()));
    }
    return new PubSubSplitEnumerator(
        ProjectSubscriptionName.of(projectName(), subscriptionName()), enumContext, assignments);
  }

  @Override
  public SimpleVersionedSerializer<SubscriptionSplit> getSplitSerializer() {
    return new SubscriptionSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<PubSubEnumeratorCheckpoint> getEnumeratorCheckpointSerializer() {
    return new PubSubCheckpointSerializer();
  }

  @Override
  public TypeInformation<OutputT> getProducedType() {
    return deserializationSchema().getProducedType();
  }

  @AutoValue.Builder
  public abstract static class Builder<OutputT> {
    public abstract Builder<OutputT> setProjectName(String projectName);

    public abstract Builder<OutputT> setSubscriptionName(String subscriptionName);

    public abstract Builder<OutputT> setDeserializationSchema(
        PubSubDeserializationSchema<OutputT> deserializationSchema);

    public abstract Builder<OutputT> setCredentials(Credentials credentials);

    public abstract PubSubSource<OutputT> build();
  }
}
