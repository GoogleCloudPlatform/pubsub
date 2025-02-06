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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.flink.PubSubDeserializationSchema;
import com.google.pubsub.flink.internal.source.split.SubscriptionSplit;
import com.google.pubsub.flink.internal.source.split.SubscriptionSplitState;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.util.HashMap;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubSourceReaderTest {
  private final TestingReaderOutput<String> output = new TestingReaderOutput<>();

  @Mock SplitReader<PubsubMessage, SubscriptionSplit> mockSplitReader;

  @Mock AckTracker mockAckTracker;

  @Mock(answer = RETURNS_DEEP_STUBS)
  SourceReaderContext mockContext;

  PubSubSourceReader<String> reader;

  @Before
  public void doBeforeEachTest() throws Exception {
    reader =
        new PubSubSourceReader<>(
            PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()),
            mockAckTracker,
            (ackTracker) -> {
              return mockSplitReader;
            },
            new Configuration(),
            mockContext);
  }

  @Test
  public void snapshot_acksOutstanding() throws Exception {
    reader.snapshotState(0L);
    verify(mockAckTracker).addCheckpoint(0L);

    reader.notifyCheckpointComplete(0L);
    verify(mockAckTracker).notifyCheckpointComplete(0L);
  }

  @Test
  public void poll_fetchesMessagesFromSplitReader() throws Exception {
    SubscriptionSplit split =
        SubscriptionSplit.create(ProjectSubscriptionName.of("project", "sub"));
    RecordsBySplits.Builder<PubsubMessage> builder = new RecordsBySplits.Builder<>();
    builder.addAll(
        split,
        ImmutableList.of(
            PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message1")).build(),
            PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message2")).build()));
    when(mockSplitReader.fetch()).thenReturn(builder.build());

    reader.addSplits(ImmutableList.of(split));
    while (output.getEmittedRecords().size() < 2) {
      reader.pollNext(output);
    }
    assertThat(output.getEmittedRecords()).containsExactly("message1", "message2");
  }

  @Test
  public void onSplitFinished_throwsError() {
    assertThrows(
        IllegalStateException.class,
        () -> reader.onSplitFinished(new HashMap<String, SubscriptionSplitState>()));
  }
}
