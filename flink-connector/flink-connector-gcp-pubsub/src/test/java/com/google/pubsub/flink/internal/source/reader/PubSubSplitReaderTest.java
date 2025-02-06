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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import com.google.pubsub.flink.internal.source.split.SubscriptionSplit;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubSplitReaderTest {

  @Mock Supplier<NotifyingPullSubscriber> mockFactory;

  @Mock NotifyingPullSubscriber mockSubscriber1;

  @Mock NotifyingPullSubscriber mockSubscriber2;

  PubSubSplitReader reader;

  @Before
  public void doBeforeEachTest() {
    reader = new PubSubSplitReader(mockFactory);
    when(mockFactory.get()).thenReturn(mockSubscriber1).thenReturn(mockSubscriber2);
  }

  private static SubscriptionSplit createSplit() {
    return SubscriptionSplit.create(ProjectSubscriptionName.of("project", "sub"));
  }

  private static Multimap<String, PubsubMessage> getSplitMessages(
      RecordsWithSplitIds<PubsubMessage> records) {
    ImmutableListMultimap.Builder<String, PubsubMessage> builder = ImmutableListMultimap.builder();
    for (String split = records.nextSplit(); split != null; split = records.nextSplit()) {
      for (PubsubMessage m = records.nextRecordFromSplit();
          m != null;
          m = records.nextRecordFromSplit()) {
        builder.put(split, m);
      }
    }
    return builder.build();
  }

  @Test
  public void noMessages_returnsEmpty() throws Throwable {
    RecordsWithSplitIds<PubsubMessage> records = reader.fetch();
    assertThat(records.finishedSplits()).isEmpty();
    assertThat(getSplitMessages(records)).isEmpty();
  }

  @Test
  public void subscriberCreateFails_propagatesError() throws Throwable {
    when(mockFactory.get()).thenThrow(new RuntimeException());

    assertThrows(
        RuntimeException.class,
        () -> reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(createSplit()))));
  }

  @Test
  public void fetchError_propagatesError() throws Throwable {
    when(mockSubscriber1.notifyDataAvailable()).thenReturn(ApiFutures.immediateFuture(null));
    doAnswer(
            invocation -> {
              throw new RuntimeException();
            })
        .when(mockSubscriber1)
        .pullMessage();
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(createSplit())));

    assertThrows(IOException.class, reader::fetch);
  }

  @Test
  public void fetch_oneSplitNoMessages() throws Throwable {
    SubscriptionSplit split1 = createSplit();
    SubscriptionSplit split2 = createSplit();
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(split1, split2)));

    PubsubMessage message =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message")).build();
    when(mockSubscriber1.notifyDataAvailable()).thenReturn(ApiFutures.immediateFuture(null));
    when(mockSubscriber2.notifyDataAvailable()).thenReturn(SettableApiFuture.create());
    when(mockSubscriber1.pullMessage()).thenReturn(Optional.of(message));
    when(mockSubscriber2.pullMessage()).thenReturn(Optional.absent());

    RecordsWithSplitIds<PubsubMessage> records = reader.fetch();
    assertThat(records.finishedSplits()).isEmpty();
    assertThat(getSplitMessages(records)).containsExactly(split1.splitId(), message);
  }

  @Test
  public void fetch_bothSplitsWithMessages() throws Throwable {
    SubscriptionSplit split1 = createSplit();
    SubscriptionSplit split2 = createSplit();
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(split1, split2)));

    PubsubMessage message1 =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message1")).build();
    PubsubMessage message2 =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message1")).build();
    when(mockSubscriber1.notifyDataAvailable()).thenReturn(ApiFutures.immediateFuture(null));
    when(mockSubscriber2.notifyDataAvailable()).thenReturn(ApiFutures.immediateFuture(null));
    when(mockSubscriber1.pullMessage()).thenReturn(Optional.of(message1));
    when(mockSubscriber2.pullMessage()).thenReturn(Optional.of(message2));

    RecordsWithSplitIds<PubsubMessage> records = reader.fetch();
    assertThat(getSplitMessages(records))
        .containsExactly(split1.splitId(), message1, split2.splitId(), message2);
  }

  @Test
  public void notifyFailure_propogatesError() throws Throwable {
    SubscriptionSplit split1 = createSplit();
    SubscriptionSplit split2 = createSplit();
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(split1, split2)));

    SettableApiFuture<Void> future1 = SettableApiFuture.create();
    SettableApiFuture<Void> future2 = SettableApiFuture.create();
    when(mockSubscriber1.notifyDataAvailable()).thenReturn(future1);
    when(mockSubscriber2.notifyDataAvailable()).thenReturn(future2);

    Future<RecordsWithSplitIds<PubsubMessage>> fetchFuture =
        Executors.newSingleThreadExecutor()
            .submit(
                () -> {
                  try {
                    return reader.fetch();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });
    assertThat(fetchFuture.isDone()).isFalse();

    future1.setException(new RuntimeException());
    assertThrows(ExecutionException.class, fetchFuture::get);
  }

  @Test
  public void interrupt_returnsEmptyMessages() throws Throwable {
    SubscriptionSplit split1 = createSplit();
    SubscriptionSplit split2 = createSplit();
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(split1, split2)));

    SettableApiFuture<Void> future1 = SettableApiFuture.create();
    SettableApiFuture<Void> future2 = SettableApiFuture.create();
    when(mockSubscriber1.notifyDataAvailable()).thenReturn(future1);
    when(mockSubscriber2.notifyDataAvailable()).thenReturn(future2);
    when(mockSubscriber1.pullMessage()).thenReturn(Optional.absent());
    when(mockSubscriber2.pullMessage()).thenReturn(Optional.absent());
    doAnswer(
            invocation -> {
              future1.setException(new PubSubNotifyingPullSubscriber.SubscriberWakeupException());
              return null;
            })
        .when(mockSubscriber1)
        .interruptNotify();
    doAnswer(
            invocation -> {
              future2.setException(new PubSubNotifyingPullSubscriber.SubscriberWakeupException());
              return null;
            })
        .when(mockSubscriber2)
        .interruptNotify();

    Future<RecordsWithSplitIds<PubsubMessage>> fetchFuture =
        Executors.newSingleThreadExecutor()
            .submit(
                () -> {
                  try {
                    return reader.fetch();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });
    assertThat(fetchFuture.isDone()).isFalse();

    reader.wakeUp();
    RecordsWithSplitIds<PubsubMessage> records = fetchFuture.get();
    assertThat(records.finishedSplits()).isEmpty();
    assertThat(getSplitMessages(records)).isEmpty();
  }

  @Test
  public void closeFailure_allSubscribersShutdown() throws Throwable {
    SubscriptionSplit split1 = createSplit();
    SubscriptionSplit split2 = createSplit();
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(split1, split2)));

    doThrow(new RuntimeException()).when(mockSubscriber1).shutdown();

    assertThrows(RuntimeException.class, reader::close);
    verify(mockSubscriber1).shutdown();
    verify(mockSubscriber2).shutdown();
  }

  @Test
  public void secondSplitAdded_isIgnored() throws Exception {
    SubscriptionSplit split1 = createSplit();
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(split1)));
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(split1)));
    verify(mockFactory, times(1)).get();
  }

  @Test
  public void unknownSplitChange_throwsError() {
    assertThrows(IllegalArgumentException.class, () -> reader.handleSplitsChanges(null));
  }
}
