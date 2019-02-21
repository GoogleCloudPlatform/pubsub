/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.google.pubsub.flic.common;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=")
public class KafkaFlags {
  @Parameter(
      names = {"--broker"},
      description = "The network address of the Kafka broker.")
  public String broker;

  @Parameter(
      names = {"--zookeeper_ip"},
      description = "The network address of the Zookeeper client.")
  public String zookeeperIp;

  @Parameter(
      names = {"--replication_factor"},
      description =
          "The replication factor to use in creating a Kafka topic, if the Kafka broker"
              + "is included as a flag.")
  public int replicationFactor = 2;

  @Parameter(
      names = {"--partitions"},
      description =
          "The number of partitions to use in creating a Kafka topic, if the "
              + "Kafka broker is included as a flag.")
  public int partitions = 100;

  private KafkaFlags() {}

  private static final KafkaFlags KAFKA_FLAGS_INSTANCE = new KafkaFlags();

  public static KafkaFlags getInstance() {
    return KAFKA_FLAGS_INSTANCE;
  }
}
