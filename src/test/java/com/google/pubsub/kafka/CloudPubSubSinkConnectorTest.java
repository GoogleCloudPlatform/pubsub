package com.google.pubsub.kafka;

import com.google.pubsub.kafka.sink.CloudPubSubSinkConnector;
import com.google.pubsub.kafka.sink.CloudPubSubSinkTask;
import com.google.pubsub.kafka.source.CloudPubSubSourceConnector;
import com.google.pubsub.kafka.source.CloudPubSubSourceTask;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

/**
 * Tests for {@link CloudPubSubSinkConnector}
 */
public class CloudPubSubSinkConnectorTest {

  private CloudPubSubSinkConnector connector;

  @Before
  public void setup() {
    connector = spy(new CloudPubSubSinkConnector());
  }

  @Test(expected = ConnectException.class)
  public void testStartException() {
    connector.start(new HashMap<>());
  }

  @Test
  public void testSourceConnectorTaskClass() {
    assertEquals(CloudPubSubSinkTask.class, connector.taskClass());
  }
}
