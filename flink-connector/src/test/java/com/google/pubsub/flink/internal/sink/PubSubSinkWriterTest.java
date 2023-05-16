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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.pubsub.flink.PubSubSerializationSchema;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.flink.api.connector.sink2.SinkWriter.Context;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubSinkWriterTest {
  @Mock FlushablePublisher mockPublisher;
  @Mock PubSubSerializationSchema<String> mockSchema;
  PubSubSinkWriter<String> sinkWriter;

  @Before
  public void doBeforeEachTest() {
    sinkWriter = new PubSubSinkWriter<>(mockPublisher, mockSchema);
  }

  @Test
  public void flush_flushesPublisher() throws Exception {
    sinkWriter.flush(false);
    verify(mockPublisher).flush();
  }

  @Test
  public void close_flushesPublisher() throws Exception {
    sinkWriter.close();
    verify(mockPublisher).flush();
  }

  @Test
  public void publish_serializesMessage() throws Exception {
    PubsubMessage message =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("data")).build();
    when(mockSchema.serialize("data")).thenReturn(message);
    sinkWriter.write(
        "data",
        new Context() {
          @Override
          public long currentWatermark() {
            return System.currentTimeMillis();
          }

          @Override
          public Long timestamp() {
            return System.currentTimeMillis();
          }
        });
    verify(mockPublisher).publish(message);
  }
}
