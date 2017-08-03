package com.google.pubsub.clients.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.ImmutableMap;
import com.google.pubsub.kafkastubs.common.serialization.Deserializer;
import com.google.pubsub.kafkastubs.common.serialization.IntegerDeserializer;
import com.google.pubsub.kafkastubs.common.serialization.StringDeserializer;
import java.util.Properties;
import org.junit.Test;

public class ConsumerConfigTest {

  @Test
  public void checkDeserializersConfiguration() {
    Properties properties = new Properties();
    properties.putAll(new ImmutableMap.Builder<>()
        .put("key.deserializer",
            "com.google.pubsub.kafkastubs.common.serialization.StringDeserializer")
        .put("value.deserializer",
            "com.google.pubsub.kafkastubs.common.serialization.IntegerDeserializer")
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
