// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////
package com.google.pubsub.kafka.source;

import com.google.common.annotations.VisibleForTesting;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SourceConnector} that writes messages to a specific topic in <a
 * href="http://kafka.apache.org/">Apache Kafka</a>.
 */
public class CloudPubSubSourceConnector extends SourceConnector {
  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSourceConnector.class);

  public static final String KAFKA_PARTITIONS_CONFIG = "kafka.partitions.count";
  public static final String KAFKA_PARTITION_SCHEME_CONFIG = "partition.scheme";
  public static final String KAFKA_MESSAGE_KEY_CONFIG = "key.attribute";
  public static final String KAFKA_TOPIC_CONFIG = "kafka.topic";
  public static final String CPS_SUBSCRIPTION_CONFIG = "subscription";
  public static final String CPS_MAX_BATCH_SIZE_CONFIG = "cps.maxBatchSize";

  private static final int DEFAULT_MAX_BATCH_SIZE = 100;
  private static final int DEFAULT_PARTITIONS = 0;
  private static final PartitionScheme DEFAULT_PARTITION_SCHEME = PartitionScheme.ROUND_ROBIN;

  private enum PartitionScheme {
    ROUND_ROBIN("round_robin"),
    HASH_KEY("hash_key"),
    HASH_VALUE("hash_value");

    private String stringRepresentation;

    PartitionScheme(String stringRepresentation) {
      this.stringRepresentation = stringRepresentation;
    }

    public String toString() {
      return stringRepresentation;
    }
  }

  private String kafkaTopic;
  private String cpsProject;
  private String cpsTopic;
  private String cpsSubscription;
  private String keyAttribute;
  private int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
  private int partitions = DEFAULT_PARTITIONS;
  private PartitionScheme partitionScheme = DEFAULT_PARTITION_SCHEME;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    kafkaTopic = ConnectorUtils.getAndValidate(props, KAFKA_TOPIC_CONFIG);
    cpsProject = ConnectorUtils.getAndValidate(props, ConnectorUtils.CPS_PROJECT_CONFIG);
    cpsTopic = ConnectorUtils.getAndValidate(props, ConnectorUtils.CPS_TOPIC_CONFIG);
    cpsSubscription = ConnectorUtils.getAndValidate(props, CPS_SUBSCRIPTION_CONFIG);
    keyAttribute = props.get(KAFKA_MESSAGE_KEY_CONFIG);
    if (props.get(CPS_MAX_BATCH_SIZE_CONFIG) != null) {
      maxBatchSize = Integer.parseInt(props.get(CPS_MAX_BATCH_SIZE_CONFIG));
    }
    if (props.get(KAFKA_PARTITIONS_CONFIG) != null) {
      partitions = Integer.parseInt(props.get(KAFKA_PARTITIONS_CONFIG));
    }
    if (props.get(KAFKA_PARTITION_SCHEME_CONFIG) != null) {
      String scheme = props.get(KAFKA_PARTITION_SCHEME_CONFIG);
      if (scheme.equals(PartitionScheme.HASH_KEY.toString())) {
        partitionScheme = PartitionScheme.HASH_KEY;
      }
      if (scheme.equals(PartitionScheme.HASH_VALUE.toString())) {
        partitionScheme = PartitionScheme.HASH_VALUE;
      }
    }
    log.info(
        "Start source connector for project "
            + cpsProject
            + " and topic "
            + cpsTopic
            + " and subscription "
            + cpsSubscription);
    verifySubscription();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return CloudPubSubSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    // Each task will get the exact same configuration.
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> config = new HashMap<>();
      config.put(KAFKA_TOPIC_CONFIG, kafkaTopic);
      config.put(ConnectorUtils.CPS_PROJECT_CONFIG, cpsProject);
      config.put(ConnectorUtils.CPS_TOPIC_CONFIG, cpsTopic);
      config.put(CPS_MAX_BATCH_SIZE_CONFIG, String.valueOf(maxBatchSize));
      config.put(CPS_SUBSCRIPTION_CONFIG, cpsSubscription);
      config.put(KAFKA_MESSAGE_KEY_CONFIG, keyAttribute);
      config.put(KAFKA_PARTITIONS_CONFIG, String.valueOf(partitions));
      config.put(KAFKA_PARTITION_SCHEME_CONFIG, partitionScheme.toString());
      configs.add(config);
    }
    return configs;
  }

  @Override
  public ConfigDef config() {
    // Defines Cloud Pub/Sub specific configurations that should be specified in the
    // properties file for this connector.
    return new ConfigDef()
        .define(
            KAFKA_TOPIC_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The topic in Kafka which will receive messages that were pulled from Cloud Pub/Sub.")
        .define(
            ConnectorUtils.CPS_PROJECT_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The project containing the topic from which to pull messages.")
        .define(
            ConnectorUtils.CPS_TOPIC_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The topic from which to pull messages")
        .define(
            CPS_SUBSCRIPTION_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The name of the subscription to Cloud Pub/Sub.")
        .define(
            CPS_MAX_BATCH_SIZE_CONFIG,
            Type.INT,
            Importance.HIGH,
            "The minimum number of messages to batch per pull request to Cloud Pub/Sub.")
        .define(
            KAFKA_MESSAGE_KEY_CONFIG,
            Type.STRING,
            Importance.MEDIUM,
            "The Cloud Pub/Sub message attribute to use as a key for messages published to Kafka.")
        .define(
            KAFKA_PARTITIONS_CONFIG,
            Type.INT,
            Importance.MEDIUM,
            "The number of partitions for the Kafka topic in which messages will be published to.")
        .define(
            KAFKA_PARTITION_SCHEME_CONFIG,
            Type.STRING,
            Importance.MEDIUM,
            "The scheme for assigning a message to a partition.");
  }

  private void verifySubscription() {
    try {
      SubscriberFutureStub stub = SubscriberGrpc.newFutureStub(ConnectorUtils.getChannel());
      GetSubscriptionRequest request =
          GetSubscriptionRequest.newBuilder()
              .setSubscription(
                  String.format(
                      ConnectorUtils.CPS_SUBSCRIPTION_FORMAT, cpsProject, cpsSubscription))
              .build();
      stub.getSubscription(request).get();
    } catch (Exception e) {
      throw new RuntimeException("The subscription " + cpsSubscription + " does not exist.");
    }
  }

  @Override
  public void stop() {}
}
