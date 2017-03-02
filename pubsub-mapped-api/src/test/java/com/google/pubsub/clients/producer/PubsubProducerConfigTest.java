package com.google.pubsub.clients.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.ImmutableMap;
import java.util.Properties;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class PubsubProducerConfigTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testSuccessAllConfigsProvided() {
    Properties props = new Properties();
    props.putAll(new ImmutableMap.Builder<>()
        .put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("project", "unit-test-project")
        .build()
    );

    PubsubProducerConfig testConfig = new PubsubProducerConfig(props);

    assertEquals("Project config equals unit-test-project.", "unit-test-project", testConfig.getString(PubsubProducerConfig.PROJECT_CONFIG));
    assertNotNull("Key serializer must not be null.", testConfig.getConfiguredInstance(PubsubProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class));
    assertNotNull("Value serializer must not be null.", testConfig.getConfiguredInstance(PubsubProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class));
  }

  @Test
  public void testNoSerializerProvided() {
    Properties props = new Properties();
    props.put(PubsubProducerConfig.PROJECT_CONFIG, "unit-test-project");

    exception.expect(ConfigException.class);
    new PubsubProducerConfig(props);

  }

  @Test
  public void testNoProjectProvided() {
    Properties props = new Properties();
    props.putAll(new ImmutableMap.Builder<>()
        .put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .build()
    );

    exception.expect(ConfigException.class);
    new PubsubProducerConfig(props);
  }
}
