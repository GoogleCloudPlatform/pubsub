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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.flink.PubSubDeserializationSchema;
import com.google.pubsub.flink.internal.source.split.SubscriptionSplitState;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.flink.api.connector.source.SourceOutput;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubRecordEmitterTest {
  @Mock PubSubDeserializationSchema mockDeserializer;
  @Mock SourceOutput<String> mockSource;
  @Mock AckTracker mockAckTracker;

  @Test
  public void emit_deserializesMessage() throws Exception {
    PubSubRecordEmitter<String> recordEmitter =
        new PubSubRecordEmitter<String>(mockDeserializer, mockAckTracker);

    PubsubMessage message =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8("message"))
            .setMessageId("message-id")
            .setPublishTime(Timestamps.fromMillis(12345L))
            .build();
    when(mockDeserializer.deserialize(message)).thenReturn("message");

    recordEmitter.emitRecord(message, mockSource, new SubscriptionSplitState(null));
    verify(mockDeserializer).deserialize(message);
    verify(mockSource).collect("message", 12345L);
    verify(mockAckTracker).stagePendingAck("message-id");
  }
}
