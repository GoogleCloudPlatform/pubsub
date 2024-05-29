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
package com.google.pubsub.flink.internal.source.reader;

import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.flink.PubSubDeserializationSchema;
import com.google.pubsub.flink.internal.source.split.SubscriptionSplit;
import com.google.pubsub.flink.internal.source.split.SubscriptionSplitState;
import com.google.pubsub.v1.PubsubMessage;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

public class PubSubSourceReader<T>
    extends SingleThreadMultiplexSourceReaderBase<
        PubsubMessage, T, SubscriptionSplit, SubscriptionSplitState> {
  public interface SplitReaderFactory {
    SplitReader<PubsubMessage, SubscriptionSplit> create(AckTracker ackTracker);
  }

  private final AckTracker ackTracker;

  public PubSubSourceReader(
      PubSubDeserializationSchema<T> schema,
      AckTracker ackTracker,
      SplitReaderFactory splitReaderFactory,
      Configuration config,
      SourceReaderContext context) {
    super(
        () -> splitReaderFactory.create(ackTracker),
        new PubSubRecordEmitter<>(schema, ackTracker),
        config,
        context);
    this.ackTracker = ackTracker;
  }

  @Override
  public List<SubscriptionSplit> snapshotState(long checkpointId) {
    ackTracker.addCheckpoint(checkpointId);
    return super.snapshotState(checkpointId);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    ackTracker.notifyCheckpointComplete(checkpointId);
  }

  @Override
  protected SubscriptionSplitState initializedState(SubscriptionSplit sourceSplit) {
    return new SubscriptionSplitState(sourceSplit);
  }

  @Override
  protected SubscriptionSplit toSplitType(String splitState, SubscriptionSplitState state) {
    return state.getSplit();
  }

  @Override
  protected void onSplitFinished(Map<String, SubscriptionSplitState> map) {
    throw new IllegalStateException(
        "Splits should never become finished, since the source is unbounded.");
  }
}
