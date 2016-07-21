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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import com.google.pubsub.kafka.common.ConnectorUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link CloudPubSubSourceConnector}. */
public class CloudPubSubSourceConnectorTest {

  private static final String CPS_TOPIC = "the";
  private static final String CPS_PROJECT = "quick";
  private static final String CPS_MAX_BATCH_SIZE = "1000";
  private static final String CPS_SUBSCRIPTION = "brown";
  private static final String KAFKA_TOPIC = "fox";
  private static final String KAFKA_MESSAGE_KEY = "jumped";

  private CloudPubSubSourceConnector connector;

  private Map<String, String> sourceProps;

  @Before
  public void setup() {
    connector = spy(new CloudPubSubSourceConnector());
    sourceProps = new HashMap<>();
    sourceProps.put(ConnectorUtils.CPS_TOPIC_CONFIG, CPS_TOPIC);
    sourceProps.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
    sourceProps.put(CloudPubSubSourceConnector.CPS_SUBSCRIPTION_CONFIG, CPS_SUBSCRIPTION);
    sourceProps.put(CloudPubSubSourceConnector.KAFKA_TOPIC_CONFIG, KAFKA_TOPIC);
    sourceProps.put(CloudPubSubSourceConnector.KAFKA_MESSAGE_KEY_CONFIG, KAFKA_MESSAGE_KEY);
  }

  @Test
  public void testStart() {
    doNothing().when(connector).verifySubscription();
    connector.start(sourceProps);
    assertEquals(connector.cpsTopic, sourceProps.get(ConnectorUtils.CPS_TOPIC_CONFIG));
    assertEquals(connector.cpsProject, sourceProps.get(ConnectorUtils.CPS_PROJECT_CONFIG));
    assertEquals(connector.maxBatchSize, CloudPubSubSourceConnector.DEFAULT_MAX_BATCH_SIZE);
    assertEquals(connector.cpsSubscription, CPS_SUBSCRIPTION);
    assertEquals(connector.kafkaTopic, KAFKA_TOPIC);
    assertEquals(connector.keyAttribute, KAFKA_MESSAGE_KEY);
  }

  @Test
  public void testStartWithBatchSizeSet() {
    doNothing().when(connector).verifySubscription();
    sourceProps.put(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG, CPS_MAX_BATCH_SIZE);
    connector.start(sourceProps);
    assertEquals(connector.cpsTopic, sourceProps.get(ConnectorUtils.CPS_TOPIC_CONFIG));
    assertEquals(connector.cpsProject, sourceProps.get(ConnectorUtils.CPS_PROJECT_CONFIG));
    int maxBatchSizeResult =
        Integer.parseInt(sourceProps.get(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG));
    assertEquals(connector.maxBatchSize, maxBatchSizeResult);
    assertEquals(connector.cpsSubscription, CPS_SUBSCRIPTION);
    assertEquals(connector.kafkaTopic, KAFKA_TOPIC);
    assertEquals(connector.keyAttribute, KAFKA_MESSAGE_KEY);
  }

  @Test(expected = RuntimeException.class)
  public void testStartExceptionCase() {
    doThrow(new RuntimeException()).when(connector).verifySubscription();

    connector.start(sourceProps);
  }

  @Test
  public void testTaskConfigs() {
    // Avoid calling connector.start()
    connector.cpsTopic = CPS_TOPIC;
    connector.cpsProject = CPS_PROJECT;
    connector.maxBatchSize = Integer.parseInt(CPS_MAX_BATCH_SIZE);
    connector.cpsSubscription = CPS_SUBSCRIPTION;
    connector.kafkaTopic = KAFKA_TOPIC;
    connector.keyAttribute = KAFKA_MESSAGE_KEY;
    List<Map<String, String>> configs = connector.taskConfigs(10);
    assertEquals(configs.size(), 10);
    for (int i = 0; i < 10; ++i) {
      assertEquals(CPS_TOPIC, configs.get(i).get(ConnectorUtils.CPS_TOPIC_CONFIG));
      assertEquals(CPS_PROJECT, configs.get(i).get(ConnectorUtils.CPS_PROJECT_CONFIG));
      assertEquals(
          CPS_MAX_BATCH_SIZE,
          configs.get(i).get(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG));
      assertEquals(
          CPS_SUBSCRIPTION, configs.get(i).get(CloudPubSubSourceConnector.CPS_SUBSCRIPTION_CONFIG));
      assertEquals(KAFKA_TOPIC, configs.get(i).get(CloudPubSubSourceConnector.KAFKA_TOPIC_CONFIG));
      assertEquals(
          KAFKA_MESSAGE_KEY,
          configs.get(i).get(CloudPubSubSourceConnector.KAFKA_MESSAGE_KEY_CONFIG));
    }
  }

  @Test
  public void testTaskClass() {
    assertEquals(CloudPubSubSourceTask.class, connector.taskClass());
  }
}
