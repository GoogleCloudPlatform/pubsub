package com.google.pubsub.kafka.common;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

/** Created by rramkumar on 7/26/16. */
public class ConnectorUtilsTests {

  private static final String TEST_KEY1 = "hello";
  private static final String TEST_KEY2 = "big";
  private static final String TEST_KEY3 = "fat";
  private static final String TEST_VALUE = "world";
  private Map<String, String> props;

  @Before
  public void setup() {
    props = new HashMap<>();
    props.put(TEST_KEY1, TEST_VALUE);
    props.put(TEST_KEY2, "");
  }

  @Test
  public void testValidateConfig() {
    assertEquals(TEST_VALUE, ConnectorUtils.validateConfig(props, TEST_KEY1));
  }

  @Test(expected = ConnectException.class)
  public void testValidateConfigWhenConfigIsEmpty() {
    ConnectorUtils.validateConfig(props, TEST_KEY2);
  }

  @Test(expected = ConnectException.class)
  public void testValidateConfigWhenNoConfigExists() {
    ConnectorUtils.validateConfig(props, TEST_KEY3);
  }
}
