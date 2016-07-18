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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

import com.google.pubsub.kafka.common.ConnectorUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link CloudPubSubSinkConnector}.
 */
public class CloudPubSubSinkConnectorTest {

  private static final String CPS_TOPIC = "test";
  private static final String CPS_PROJECT = "test";
  private static final String CPS_MIN_BATCH_SIZE = "1000";

  private CloudPubSubSinkConnector connector;
  private Map<String, String> sinkProps;

  @Before
  public void setup() {
    connector = spy(new CloudPubSubSinkConnector());
    sinkProps = new HashMap<>();
    sinkProps.put(ConnectorUtils.CPS_TOPIC_CONFIG, CPS_TOPIC);
    sinkProps.put(ConnectorUtils.CPS_PROJECT_CONFIG, CPS_PROJECT);
  }

  @Test
  public void testStart() {
    connector.start(sinkProps);
    assertEquals(connector.cpsTopic, sinkProps.get(ConnectorUtils.CPS_TOPIC_CONFIG));
    assertEquals(connector.cpsProject, sinkProps.get(ConnectorUtils.CPS_PROJECT_CONFIG));
    assertEquals(connector.minBatchSize, CloudPubSubSinkConnector.DEFAULT_MIN_BATCH_SIZE);
  }

  @Test
  public void testStartWithBatchSizeSet() {
    sinkProps.put(CloudPubSubSinkConnector.CPS_MIN_BATCH_SIZE_CONFIG, CPS_MIN_BATCH_SIZE);
    connector.start(sinkProps);
    assertEquals(connector.cpsTopic, sinkProps.get(ConnectorUtils.CPS_TOPIC_CONFIG));
    assertEquals(connector.cpsProject, sinkProps.get(ConnectorUtils.CPS_PROJECT_CONFIG));
    int minBatchSizeResult = Integer.parseInt(
        sinkProps.get(CloudPubSubSinkConnector.CPS_MIN_BATCH_SIZE_CONFIG));
    assertEquals(connector.minBatchSize, minBatchSizeResult);
  }

  @Test
  public void testTaskConfigs() {
    // Avoid calling connector.start().
    connector.cpsTopic = CPS_TOPIC;
    connector.cpsProject = CPS_PROJECT;
    connector.minBatchSize = Integer.parseInt(CPS_MIN_BATCH_SIZE);
    List<Map<String, String>> configs = connector.taskConfigs(10);
    assertEquals(configs.size(), 10);
    for (int i = 0; i < 10; ++i) {
      assertEquals(CPS_TOPIC, configs.get(i).get(ConnectorUtils.CPS_TOPIC_CONFIG));
      assertEquals(CPS_PROJECT, configs.get(i).get(ConnectorUtils.CPS_PROJECT_CONFIG));
      assertEquals(CPS_MIN_BATCH_SIZE,
          configs.get(i).get(CloudPubSubSinkConnector.CPS_MIN_BATCH_SIZE_CONFIG));
    }
  }

  @Test
  public void testTaskClass() {
    assertEquals(CloudPubSubSinkTask.class, connector.taskClass());
  }
}
