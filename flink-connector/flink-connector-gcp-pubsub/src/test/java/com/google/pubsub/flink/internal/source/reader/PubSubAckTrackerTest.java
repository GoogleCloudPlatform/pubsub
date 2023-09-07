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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubAckTrackerTest {
  PubSubAckTracker ackTracker;

  @Before
  public void doBeforeEachTest() {
    ackTracker = new PubSubAckTracker();
  }

  @Test
  public void singleAck_ackedOnCheckpoint() throws Exception {
    AckReplyConsumer mockAck = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck(mockAck);
    ackTracker.addCheckpoint(1L);
    ackTracker.notifyCheckpointComplete(1L);
    verify(mockAck).ack();
  }

  @Test
  public void manyAcks_ackedOnCheckpoint() throws Exception {
    AckReplyConsumer mockAck1 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck(mockAck1);
    AckReplyConsumer mockAck2 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck(mockAck2);
    ackTracker.addCheckpoint(1L);
    ackTracker.notifyCheckpointComplete(1L);
    verify(mockAck1).ack();
    verify(mockAck2).ack();
  }

  @Test
  public void manyCheckpoints_completedOneByOne() throws Exception {
    AckReplyConsumer mockAck1 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck(mockAck1);
    ackTracker.addCheckpoint(1L);

    AckReplyConsumer mockAck2 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck(mockAck2);
    ackTracker.addCheckpoint(2L);

    ackTracker.notifyCheckpointComplete(1L);
    verify(mockAck1).ack();

    ackTracker.notifyCheckpointComplete(2L);
    verify(mockAck2).ack();
  }

  @Test
  public void manyCheckpoints_completedTogether() throws Exception {
    AckReplyConsumer mockAck1 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck(mockAck1);
    ackTracker.addCheckpoint(1L);

    AckReplyConsumer mockAck2 = mock(AckReplyConsumer.class);
    ackTracker.addPendingAck(mockAck2);
    ackTracker.addCheckpoint(2L);

    ackTracker.notifyCheckpointComplete(2L);
    verify(mockAck1).ack();
    verify(mockAck2).ack();
  }
}
