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

import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A simple example PubSub Flink job.
 *
 * <p>This job pulls messages from a PubSub subscription as a data source and publishes these
 * messages to a PubSub topic as a sink.
 *
 * <p>Example usage (source subscription and sink topic must be in the same GCP project): $ flink
 * run PubSubExample.jar --project PROJECT-NAME --source-subscription SUBSCRIPTION-NAME --sink-topic
 * TOPIC-NAME
 *
 * <p>Example usage (source subscription and sink topic can be in different GCP projects): $ flink
 * run PubSubExample.jar --source-subscription projects/PROJECT-NAME/subscriptions/SUBSCRIPTION-NAME
 * --sink-topic projects/PROJECT-NAME/topics/TOPIC-NAME
 */
public class PubSubExample {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);
    // Parse sink topic from parameters.
    String topicName = parameterTool.get("sink-topic");
    String topicProject = parameterTool.get("project");
    if (topicName == null) {
      System.out.println("Failed to start! The parameter --sink-topic must be specified.");
    }
    if (ProjectTopicName.isParsableFrom(topicName)) {
      ProjectTopicName projectTopicName = ProjectTopicName.parse(topicName);
      topicName = projectTopicName.getTopic();
      topicProject = projectTopicName.getProject();
    }
    if (topicProject == null) {
      System.out.println(
          "Failed to start! The sink topic project must be specified using either [--project"
              + " PROJECT-NAME] or [--sink-topic projects/PROJECT-NAME/topics/TOPIC-NAME].");
      return;
    }
    // Parse source subscription from parameters.
    String subscriptionName = parameterTool.get("source-subscription");
    String subscriptionProject = parameterTool.get("project");
    if (subscriptionName == null) {
      System.out.println("Failed to start! The parameter --source-subscription must be specified.");
    }
    if (ProjectSubscriptionName.isParsableFrom(subscriptionName)) {
      ProjectSubscriptionName projectSubscriptionName =
          ProjectSubscriptionName.parse(subscriptionName);
      subscriptionName = projectSubscriptionName.getSubscription();
      subscriptionProject = projectSubscriptionName.getProject();
    }
    if (subscriptionProject == null) {
      System.out.println(
          "Failed to start! The source subscription project must be specified using either"
              + " [--project PROJECT-NAME] or [--source-subscription"
              + " projects/PROJECT-NAME/subscriptions/SUBSCRIPTION-NAME].");
      return;
    }

    DataStream<String> stream =
        env.fromSource(
            PubSubSource.<String>builder()
                .setDeserializationSchema(
                    PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                .setProjectName(subscriptionProject)
                .setSubscriptionName(subscriptionName)
                .build(),
            WatermarkStrategy.noWatermarks(),
            "PubSubSource");
    stream
        .sinkTo(
            PubSubSink.<String>builder()
                .setSerializationSchema(
                    PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
                .setProjectName(topicProject)
                .setTopicName(topicName)
                .build())
        .name("PubSubSink");

    // Start a checkpoint every 1000 ms.
    env.enableCheckpointing(1000);
    env.execute("Streaming PubSub Example");
  }
}
