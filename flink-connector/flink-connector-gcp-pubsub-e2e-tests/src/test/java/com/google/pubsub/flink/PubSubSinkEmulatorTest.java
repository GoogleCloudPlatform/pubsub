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
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** E2E test for PubSubSink using a local Pub/Sub emulator. */
public class PubSubSinkEmulatorTest extends TestLogger {
  private final TopicName topic = TopicName.of("test-project", "test-topic");
  private final SubscriptionName subscription =
      SubscriptionName.of("test-project", "test-subscription");

  @Test
  public void testPubSubSink() throws Exception {
    PubSubEmulatorHelper.createTopic(topic);
    PubSubEmulatorHelper.createSubscription(subscription, topic);
    List<String> messageInput = Arrays.asList("msg-1", "msg-2", "msg-3", "msg-4", "msg-5");
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(100);
    env.setParallelism(1);
    DataStream<String> stream = env.fromCollection(messageInput);
    stream
        .sinkTo(
            PubSubSink.<String>builder()
                .setSerializationSchema(
                    PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
                .setProjectName(topic.getProject())
                .setTopicName(topic.getTopic())
                .setEndpoint(
                    EmulatorEndpoint.toEmulatorEndpoint(PubSubEmulatorHelper.getEmulatorEndpoint()))
                .build())
        .name("PubSubSink");
    env.execute("PubSubSinkEmulatorTest");

    List<PubsubMessage> messageOutput =
        PubSubEmulatorHelper.pullAndAckMessages(
            subscription,
            /* expectedMessageCount= */ messageInput.size(),
            /* deadlineSeconds= */ 60);
    List<String> messageDataOutput =
        messageOutput.stream()
            .map(message -> message.getData().toStringUtf8())
            .collect(Collectors.toList());

    assertEquals(
        "Did not receive expected number of messages.", messageInput.size(), messageOutput.size());
    for (final String msg : messageInput) {
      assertTrue("Missing " + msg, messageDataOutput.contains(msg));
    }
  }
}
