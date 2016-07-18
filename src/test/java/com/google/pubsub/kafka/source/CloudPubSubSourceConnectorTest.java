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
import org.apache.kafka.connect.connector.ConnectorContext;

import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/**
 * Tests for {@link CloudPubSubSourceConnector}.
 */
public class CloudPubSubSourceConnectorTest {

  private static final String CPS_TOPIC = "test";
  private static final String CPS_PROJECT = "test";
  private static final String CPS_MAX_BATCH_SIZE = "1000";
  private static final String SUBSCRIPTION_NAME = "testsubscription";

  private CloudPubSubSourceConnector connector;

  private Map<String, String> sourceProps;

  @Before
  public void setup() {
    connector = spy(new CloudPubSubSourceConnector());
    sourceProps = new HashMap<>();
    sourceProps.put(ConnectorUtils.CPS_TOPIC_CONFIG, CPS_TOPIC);
    sourceProps.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
  }

  @Test
  public void testStart() {
    doReturn(SUBSCRIPTION_NAME).when(connector).createSubscription();
    connector.start(sourceProps);
    assertEquals(connector.cpsTopic, sourceProps.get(ConnectorUtils.CPS_TOPIC_CONFIG));
    assertEquals(connector.cpsProject, sourceProps.get(ConnectorUtils.CPS_PROJECT_CONFIG));
    assertEquals(connector.maxBatchSize, CloudPubSubSourceConnector.DEFAULT_MAX_BATCH_SIZE);
    assertEquals(connector.subscriptionName, SUBSCRIPTION_NAME);
  }

  @Test
  public void testStartWithBatchSizeSet() {
    doReturn(SUBSCRIPTION_NAME).when(connector).createSubscription();
    sourceProps.put(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG, CPS_MAX_BATCH_SIZE);
    connector.start(sourceProps);
    assertEquals(connector.cpsTopic, sourceProps.get(ConnectorUtils.CPS_TOPIC_CONFIG));
    assertEquals(connector.cpsProject, sourceProps.get(ConnectorUtils.CPS_PROJECT_CONFIG));
    int maxBatchSizeResult = Integer.parseInt(
        sourceProps.get(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG));
    assertEquals(connector.maxBatchSize, maxBatchSizeResult);
    assertEquals(connector.subscriptionName, SUBSCRIPTION_NAME);
  }

  @Test(expected = RuntimeException.class)
  public void testStartExceptionCase() {
    doThrow(new RuntimeException()).when(connector).createSubscription();
    connector.start(sourceProps);
  }

  @Test
  public void testTaskConfigs() {
    // Avoid calling connector.start()
    connector.cpsTopic = CPS_TOPIC;
    connector.cpsProject = CPS_PROJECT;
    connector.maxBatchSize = Integer.parseInt(CPS_MAX_BATCH_SIZE);
    connector.subscriptionName = SUBSCRIPTION_NAME;
    List<Map<String, String>> configs = connector.taskConfigs(10);
    assertEquals(configs.size(), 10);
    for (int i = 0; i < 10; ++i) {
      assertEquals(CPS_TOPIC, configs.get(i).get(ConnectorUtils.CPS_TOPIC_CONFIG));
      assertEquals(CPS_PROJECT, configs.get(i).get(ConnectorUtils.CPS_PROJECT_CONFIG));
      assertEquals(CPS_MAX_BATCH_SIZE,
          configs.get(i).get(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG));
      assertEquals(SUBSCRIPTION_NAME,
          configs.get(i).get(CloudPubSubSourceConnector.SUBSCRIPTION_NAME));
    }
  }

  @Test
  public void testTaskClass() {
    assertEquals(CloudPubSubSourceTask.class, connector.taskClass());
  }
}
