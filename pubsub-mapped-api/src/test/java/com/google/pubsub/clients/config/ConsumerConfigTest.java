package com.google.pubsub.clients.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.ImmutableMap;
import java.util.Properties;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

public class ConsumerConfigTest {

  @Test
  public void checkDeserializersConfiguration() {
    Properties properties = new Properties();
    properties.putAll(new ImmutableMap.Builder<>()
        .put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer")
        .put("value.deserializer",
            "org.apache.kafka.common.serialization.IntegerDeserializer")
        .put("max.poll.records", 500)
        .build()
    );

    ConsumerConfig consumerConfig = new ConsumerConfig(properties);

    Deserializer keyDeserializer = consumerConfig
        .getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Deserializer.class);

    Deserializer valueDeserializer = consumerConfig
        .getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class);

    assertNotNull(keyDeserializer);
    assertEquals(StringDeserializer.class, keyDeserializer.getClass());

    assertNotNull(valueDeserializer);
    assertEquals(IntegerDeserializer.class, valueDeserializer.getClass());
  }

}
