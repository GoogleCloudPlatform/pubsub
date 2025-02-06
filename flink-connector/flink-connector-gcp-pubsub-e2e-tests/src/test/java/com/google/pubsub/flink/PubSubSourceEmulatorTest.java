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

import com.google.pubsub.flink.util.EmulatorEndpoint;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** E2E test for PubSubSource using a local Pub/Sub emulator. */
public class PubSubSourceEmulatorTest extends TestLogger {
  private final TopicName topic = TopicName.of("test-project", "test-topic");
  private final SubscriptionName subscription =
      SubscriptionName.of("test-project", "test-subscription");

  @Test
  public void testPubSubSource() throws Exception {
    PubSubEmulatorHelper.createTopic(topic);
    PubSubEmulatorHelper.createSubscription(subscription, topic);
    List<String> messageInput = Arrays.asList("msg-1", "msg-2", "msg-3", "msg-4", "msg-5");
    PubSubEmulatorHelper.publishMessages(topic, messageInput);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(100);
    env.setParallelism(2);

    DataStream<String> stream =
        env.fromSource(
            PubSubSource.<String>builder()
                .setDeserializationSchema(
                    PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                .setProjectName(subscription.getProject())
                .setSubscriptionName(subscription.getSubscription())
                .setEndpoint(
                    EmulatorEndpoint.toEmulatorEndpoint(PubSubEmulatorHelper.getEmulatorEndpoint()))
                .build(),
            WatermarkStrategy.noWatermarks(),
            "PubSubEmulatorSource");
    CloseableIterator<String> iterator = stream.executeAndCollect();
    List<String> messageOutput = new ArrayList<>();
    while (messageOutput.size() < messageInput.size() && iterator.hasNext()) {
      messageOutput.add(iterator.next());
    }
    iterator.close();

    assertEquals(
        "Did not receive expected number of messages.", messageInput.size(), messageOutput.size());
    for (final String msg : messageInput) {
      assertTrue("Missing " + msg, messageOutput.contains(msg));
    }
  }
}
