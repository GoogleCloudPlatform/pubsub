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
package com.google.pubsub.flink.internal.sink;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubFlushablePublisherTest {
  @Mock Publisher mockPublisher;
  PubSubFlushablePublisher flushablePublisher;

  @Before
  public void doBeforeEachTest() {
    flushablePublisher = new PubSubFlushablePublisher(mockPublisher);
  }

  @Test
  public void publish_success() throws Exception {
    PubsubMessage message =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message")).build();
    when(mockPublisher.publish(message)).thenReturn(ApiFutures.immediateFuture("ack-id"));

    flushablePublisher.publish(message);
    verify(mockPublisher).publish(message);

    flushablePublisher.flush();
  }

  @Test
  public void publish_singleFailure() throws Exception {
    PubsubMessage message1 =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message1")).build();
    when(mockPublisher.publish(message1)).thenReturn(ApiFutures.immediateFuture("ack-id"));
    PubsubMessage message2 =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message2")).build();
    when(mockPublisher.publish(message2))
        .thenReturn(ApiFutures.immediateFailedFuture(new RuntimeException()));

    flushablePublisher.publish(message1);
    verify(mockPublisher).publish(message1);
    flushablePublisher.publish(message2);
    verify(mockPublisher).publish(message2);

    assertThrows(RuntimeException.class, flushablePublisher::flush);
  }

  @Test
  public void flush_blocksUntilPublishesComplete() throws Exception {
    PubsubMessage message =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("message")).build();
    SettableApiFuture<String> messageFuture = SettableApiFuture.create();
    when(mockPublisher.publish(message)).thenReturn(messageFuture);

    flushablePublisher.publish(message);
    verify(mockPublisher).publish(message);

    Future<?> flushFuture =
        Executors.newSingleThreadExecutor().submit(() -> flushablePublisher.flush());
    assertThat(flushFuture.isDone()).isFalse();

    messageFuture.set("ack-id");
    flushFuture.get();
  }
}
