package com.google.pubsub.kafka;

import org.junit.Before;
import org.junit.Test;

import com.google.pubsub.kafka.source.CloudPubSubSourceConnector;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link CloudPubSubSourceConnector}.
 */
public class CloudPubSubSourceConnectorTest {

  CloudPubSubSourceConnector connector = new CloudPubSubSourceConnector();

  @Before
  public void setup() {

  }

  @Test
  public void testStart() {
    Map<String, String> props = new HashMap<>();
  }
}
