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

import java.util.logging.Logger;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A simple PubSub example Flink job that prints messages from a PubSub subscription.
 *
 * <p>Example usage: --project project-name --subscription source-subscription
 */
public class PubSubSourcePrintExample {
  private static final Logger log = Logger.getLogger(PubSubSourcePrintExample.class.getName());

  public static void main(String[] args) throws Exception {
    // Parse input parameters.
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);

    if (parameterTool.getNumberOfParameters() != 2) {
      System.out.println(
          "Missing parameters!\n"
              + "Usage: flink run PubSubSourcePrintExample.jar --project <GCP project name>"
              + " --subscription <subscription>");
      return;
    }

    String projectName = parameterTool.getRequired("project");
    String subscription = parameterTool.getRequired("subscription");

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    PubSubDeserializationSchema<String> schema =
        PubSubDeserializationSchema.dataOnly(new SimpleStringSchema());
    PubSubSource.Builder<String> sourceBuilder =
        PubSubSource.<String>builder()
            .setDeserializationSchema(schema)
            .setProjectName(projectName)
            .setSubscriptionName(subscription);

    DataStream<String> stream =
        env.fromSource(
            sourceBuilder.build(), WatermarkStrategy.noWatermarks(), "ExamplePubsubSource");

    // Depending on the system where this code is executed, stream.print() may not be supported, so
    // we use this as a workaround. Furthermore, this logging can be integrated with GCP Cloud
    // Logging. See https://cloud.google.com/logging/docs/setup/java#example_2.
    stream.map(
        new MapFunction<String, Integer>() {
          @Override
          public Integer map(String s) throws Exception {
            log.info(s);
            return 0;
          }
        });

    env.enableCheckpointing(10);
    env.setParallelism(2);

    env.execute("Flink Streaming PubSubReader");
  }
}
