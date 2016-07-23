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
package com.google.pubsub.kafka.sink;

import com.google.pubsub.kafka.common.ConnectorUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SinkConnector} that writes messages to a specified topic in <a
 * href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>.
 */
public class CloudPubSubSinkConnector extends SinkConnector {

  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSinkConnector.class);

  public static final String CPS_MIN_BATCH_SIZE_CONFIG = "cps.minBatchSize";

  private static final int DEFAULT_MIN_BATCH_SIZE = 100;

  private String cpsProject;
  private String cpsTopic;
  private int minBatchSize = DEFAULT_MIN_BATCH_SIZE;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    cpsProject = ConnectorUtils.getAndValidate(props, ConnectorUtils.CPS_PROJECT_CONFIG);
    cpsTopic = ConnectorUtils.getAndValidate(props, ConnectorUtils.CPS_TOPIC_CONFIG);
    if (props.get(CPS_MIN_BATCH_SIZE_CONFIG) != null) {
      minBatchSize = Integer.parseInt(props.get(CPS_MIN_BATCH_SIZE_CONFIG));
    }
    log.info("Start sink connector for project " + cpsProject + " and topic " + cpsTopic);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return CloudPubSubSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    // Each task will get the exact same configuration.
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> config = new HashMap<>();
      config.put(ConnectorUtils.CPS_PROJECT_CONFIG, cpsProject);
      config.put(ConnectorUtils.CPS_TOPIC_CONFIG, cpsTopic);
      config.put(CPS_MIN_BATCH_SIZE_CONFIG, String.valueOf(minBatchSize));
      configs.add(config);
    }
    return configs;
  }

  @Override
  public ConfigDef config() {
    // Defines Cloud Pub/Sub specific configurations.
    return new ConfigDef()
        .define(
            ConnectorUtils.CPS_PROJECT_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The project containing the topic to which to publish.")
        .define(
            ConnectorUtils.CPS_TOPIC_CONFIG,
            Type.STRING,
            Importance.HIGH,
            "The topic to which to publish.")
        .define(
            CPS_MIN_BATCH_SIZE_CONFIG,
            Type.INT,
            Importance.HIGH,
            "The minimum number of messages to batch per partition before sending a publish "
                + "request to Cloud Pub/Sub.");
  }

  @Override
  public void stop() {}
}
