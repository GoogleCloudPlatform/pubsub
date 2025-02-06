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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubNotifyingPullSubscriberTest {
  @Mock AckTracker mockAckTracker;
  @Mock Subscriber mockSubscriber;

  PubSubNotifyingPullSubscriber pullSubscriber;

  MessageReceiver receiver;

  @Before
  public void doBeforeEachTest() {
    when(mockSubscriber.startAsync()).thenReturn(mockSubscriber);
    pullSubscriber =
        new PubSubNotifyingPullSubscriber(
            (MessageReceiver receiver) -> {
              this.receiver = receiver;
              return mockSubscriber;
            },
            mockAckTracker);
  }

  @Test
  public void notify_blocksUntilMessagesReceived() throws Throwable {
    ApiFuture<Void> notification = pullSubscriber.notifyDataAvailable();
    assertThat(notification.isDone()).isFalse();

    PubsubMessage message =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8("message"))
            .setMessageId("message-id")
            .build();
    AckReplyConsumer consumer = mock(AckReplyConsumer.class);
    receiver.receiveMessage(message, consumer);

    assertThat(notification.isDone()).isTrue();
    assertThat(pullSubscriber.pullMessage().get()).isEqualTo(message);
    verify(mockAckTracker).addPendingAck("message-id", consumer);
  }

  @Test
  public void notify_completesImmediatelyIfMessagesAvailable() throws Throwable {
    PubsubMessage message =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message")).build();
    AckReplyConsumer consumer = mock(AckReplyConsumer.class);
    receiver.receiveMessage(message, consumer);

    ApiFuture<Void> notification = pullSubscriber.notifyDataAvailable();
    assertThat(notification.isDone()).isTrue();
  }

  @Test
  public void messages_pulledInOrder() throws Throwable {
    PubsubMessage message1 =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8("message1"))
            .setMessageId("message1-id")
            .build();
    PubsubMessage message2 =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8("message2"))
            .setMessageId("message2-id")
            .build();
    PubsubMessage message3 =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8("message3"))
            .setMessageId("message3-id")
            .build();

    AckReplyConsumer consumer1 = mock(AckReplyConsumer.class);
    AckReplyConsumer consumer2 = mock(AckReplyConsumer.class);
    AckReplyConsumer consumer3 = mock(AckReplyConsumer.class);

    receiver.receiveMessage(message1, consumer1);
    receiver.receiveMessage(message2, consumer2);
    receiver.receiveMessage(message3, consumer3);

    verify(mockAckTracker).addPendingAck("message1-id", consumer1);
    verify(mockAckTracker).addPendingAck("message2-id", consumer2);
    verify(mockAckTracker).addPendingAck("message3-id", consumer3);
    assertThat(pullSubscriber.pullMessage().get()).isEqualTo(message1);
    assertThat(pullSubscriber.pullMessage().get()).isEqualTo(message2);
    assertThat(pullSubscriber.pullMessage().get()).isEqualTo(message3);
  }

  @Test
  public void noMessagesAvailable_returnsEmpty() throws Throwable {
    assertThat(pullSubscriber.pullMessage().isPresent()).isFalse();
  }

  @Test
  public void permanentError_failsSubscriber() throws Throwable {
    pullSubscriber.setPermanentError(new RuntimeException());

    assertThrows(RuntimeException.class, () -> pullSubscriber.pullMessage());
    assertThrows(ExecutionException.class, () -> pullSubscriber.notifyDataAvailable().get());
  }

  @Test
  public void interrupt_completesNotification() throws Throwable {
    ApiFuture<Void> notification = pullSubscriber.notifyDataAvailable();
    assertThat(notification.isDone()).isFalse();

    pullSubscriber.interruptNotify();
    assertThat(notification.isDone()).isTrue();
    assertThrows(
        PubSubNotifyingPullSubscriber.SubscriberWakeupException.class,
        () -> {
          try {
            notification.get();
          } catch (ExecutionException e) {
            throw e.getCause();
          }
        });
  }

  @Test
  public void shutdown_failsSusbcriber() throws Throwable {
    when(mockSubscriber.stopAsync()).thenReturn(mockSubscriber);
    pullSubscriber.shutdown();
    verify(mockSubscriber).stopAsync();
    verify(mockAckTracker).nackAll();

    assertThrows(Exception.class, () -> pullSubscriber.pullMessage());
    assertThrows(ExecutionException.class, () -> pullSubscriber.notifyDataAvailable().get());
  }

  @Test
  public void shutdown_nacksOutstandingMessages() throws Throwable {
    PubsubMessage message1 =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8("message1"))
            .setMessageId("message1-id")
            .build();
    PubsubMessage message2 =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8("message2"))
            .setMessageId("message2-id")
            .build();
    AckReplyConsumer consumer1 = mock(AckReplyConsumer.class);
    AckReplyConsumer consumer2 = mock(AckReplyConsumer.class);
    receiver.receiveMessage(message1, consumer1);
    receiver.receiveMessage(message2, consumer2);
    verify(mockAckTracker).addPendingAck("message1-id", consumer1);
    verify(mockAckTracker).addPendingAck("message2-id", consumer2);

    assertThat(pullSubscriber.pullMessage().get()).isEqualTo(message1);
    when(mockSubscriber.stopAsync()).thenReturn(mockSubscriber);
    pullSubscriber.shutdown();
    verify(mockSubscriber).stopAsync();

    assertThrows(Exception.class, () -> pullSubscriber.pullMessage());
    assertThrows(ExecutionException.class, () -> pullSubscriber.notifyDataAvailable().get());
    verify(mockAckTracker).nackAll();
  }
}
