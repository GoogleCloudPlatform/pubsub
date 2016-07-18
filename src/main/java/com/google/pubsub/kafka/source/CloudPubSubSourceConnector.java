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

import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.Subscription;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link SourceConnector} that writes messages to a specific topic in
 * <a href="http://kafka.apache.org/">Apache Kafka</a>.
 */
public class CloudPubSubSourceConnector extends SourceConnector {
  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSourceConnector.class);

  public static final String CPS_MAX_BATCH_SIZE_CONFIG = "cps.maxBatchSize";
  // Not a connector config, rather each task needs this subscription name to pull messages.
  public static final String SUBSCRIPTION_NAME = "subscription_name";

  protected static final int DEFAULT_MAX_BATCH_SIZE = 100;

  protected String cpsProject;
  protected String cpsTopic;
  protected int maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
  protected String subscriptionName;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    cpsProject = props.get(ConnectorUtils.CPS_PROJECT_CONFIG);
    cpsTopic = props.get(ConnectorUtils.CPS_TOPIC_CONFIG);
    if (props.get(CPS_MAX_BATCH_SIZE_CONFIG) != null) {
      maxBatchSize = Integer.parseInt(props.get(CPS_MAX_BATCH_SIZE_CONFIG));
    }
    subscriptionName = createSubscription();
    log.info("Start source connector for project " + cpsProject + " and topic " + cpsTopic);
    createSubscription();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return CloudPubSubSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    // Each task will get the exact same configurations.
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> config = new HashMap<>();
      config.put(ConnectorUtils.CPS_PROJECT_CONFIG, cpsProject);
      config.put(ConnectorUtils.CPS_TOPIC_CONFIG, cpsTopic);
      config.put(CPS_MAX_BATCH_SIZE_CONFIG, String.valueOf(maxBatchSize));
      config.put(SUBSCRIPTION_NAME, subscriptionName);
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
            ConnectorUtils.CPS_PROJECT_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The project containing the topic to which to publish.")
        .define(
            ConnectorUtils.CPS_TOPIC_CONFIG,
            Type.STRING, Importance.HIGH,
            "The topic to " + "which to publish.")
        .define(
            CPS_MAX_BATCH_SIZE_CONFIG,
            Type.INT,
            Importance.HIGH,
            "The minimum number of messages to batch per pull request to Cloud Pub/Sub.");

  }

  @Override
  public void stop() {
    // TODO(rramkumar): Find out how to implement this.
  }

  protected String createSubscription() {
    try {
      SubscriberGrpc.SubscriberFutureStub stub = SubscriberGrpc.newFutureStub(
          ConnectorUtils.getChannel());
      Subscription request = Subscription.newBuilder()
          .setTopic(String.format(ConnectorUtils.CPS_TOPIC_FORMAT, cpsProject, cpsTopic))
          .build();
      return stub.createSubscription(request).get().getName();
    } catch (Exception e) {
      throw new RuntimeException("Could not subscribe to the specified CPS topic: " + e);
    }
  }
}

