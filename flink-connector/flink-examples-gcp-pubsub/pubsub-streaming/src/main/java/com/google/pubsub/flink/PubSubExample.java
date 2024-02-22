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
 * <p>Example usage: --project project-name --subscription source-subscription --topic sink-topic
 */
public class PubSubExample {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Parse input parameters.
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);
    if (parameterTool.getNumberOfParameters() != 3) {
      System.out.println(
          "Missing parameters!\n"
              + "Usage: flink run PubSubExample.jar --project <GCP project name>"
              + " --subscription <subscription> --topic <topic>");
      return;
    }
    String projectName = parameterTool.getRequired("project");
    String subscription = parameterTool.getRequired("subscription");
    String topic = parameterTool.getRequired("topic");

    DataStream<String> stream =
        env.fromSource(
            PubSubSource.<String>builder()
                .setDeserializationSchema(
                    PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                .setProjectName(projectName)
                .setSubscriptionName(subscription)
                .build(),
            WatermarkStrategy.noWatermarks(),
            "PubSubSource");
    stream
        .sinkTo(
            PubSubSink.<String>builder()
                .setSerializationSchema(
                    PubSubSerializationSchema.dataOnly(new SimpleStringSchema()))
                .setProjectName(projectName)
                .setTopicName(topic)
                .build())
        .name("PubSubSink");

    // Start a checkpoint every 1000 ms.
    env.enableCheckpointing(1000);
    env.execute("Streaming PubSub Example");
  }
}
