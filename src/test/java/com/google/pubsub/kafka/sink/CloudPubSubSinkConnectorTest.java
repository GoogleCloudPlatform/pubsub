package com.google.pubsub.kafka.sink;

import com.google.pubsub.kafka.common.ConnectorUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/**
 * Created by rramkumar on 7/16/16.
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
