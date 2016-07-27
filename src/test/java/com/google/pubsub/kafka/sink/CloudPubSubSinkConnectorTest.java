package com.google.pubsub.kafka.sink;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link CloudPubSubSinkConnector}. */
public class CloudPubSubSinkConnectorTest {

  private static final String TEST_KEY = "hello";
  private static final String TEST_VALUE = "world";
  private static final int NUM_TASKS = 10;

  private CloudPubSubSinkConnector connector;
  private Map<String, String> props;

  @Before
  public void setup() {
    connector = spy(new CloudPubSubSinkConnector());
    props = new HashMap<>();
    props.put(TEST_KEY, TEST_VALUE);
  }

  @Test
  public void testTaskConfigs() {
    connector.start(props);
    List<Map<String, String>> taskConfigs = connector.taskConfigs(NUM_TASKS);
    assertEquals(taskConfigs.size(), NUM_TASKS);
    for (int i = 0; i < taskConfigs.size(); ++i) {
      assertEquals(taskConfigs.get(i), props);
    }
  }

  @Test
  public void testTaskClass() {
    assertEquals(CloudPubSubSinkTask.class, connector.taskClass());
  }
}
