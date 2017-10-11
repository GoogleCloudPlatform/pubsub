/* Copyright 2017 Google Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License. */

package com.google.pubsub.clients.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

public class ConfigTest {

  @Test
  public void checkPropertiesDeserializersConfiguration() {
    Config config = new Config(getProperties());

    assertNotNull(config.getKeyDeserializer());
    assertEquals(StringDeserializer.class, config.getKeyDeserializer().getClass());

    assertNotNull(config.getValueDeserializer());
    assertEquals(IntegerDeserializer.class, config.getValueDeserializer().getClass());
  }

  @Test
  public void checkPropertiesDeserializersConfigurationDeserializersProvided() {
    Config config = new Config<>(getProperties(), new DoubleDeserializer(), new ByteArrayDeserializer());

    assertNotNull(config.getKeyDeserializer());
    assertEquals(DoubleDeserializer.class, config.getKeyDeserializer().getClass());

    assertNotNull(config.getValueDeserializer());
    assertEquals(ByteArrayDeserializer.class, config.getValueDeserializer().getClass());
  }

  @Test
  public void checkConfigMapDeserializersConfiguration() {
    Config config = new Config(getConfigMap());

    assertNotNull(config.getKeyDeserializer());
    assertEquals(StringDeserializer.class, config.getKeyDeserializer().getClass());

    assertNotNull(config.getValueDeserializer());
    assertEquals(IntegerDeserializer.class, config.getValueDeserializer().getClass());
  }

  @Test
  public void checkConfigMapDeserializersConfigurationDeserializersProvided() {
    Config config = new Config<>(getConfigMap(), new LongDeserializer(), new StringDeserializer());

    assertNotNull(config.getKeyDeserializer());
    assertEquals(LongDeserializer.class, config.getKeyDeserializer().getClass());

    assertNotNull(config.getValueDeserializer());
    assertEquals(StringDeserializer.class, config.getValueDeserializer().getClass());
  }

  private Map<String, Object> getConfigMap() {
    Map<String, Object> properties = new HashMap<>();
    properties.putAll(getTestOptionsMap());
    return properties;
  }

  private Properties getProperties() {
    Properties properties = new Properties();
    properties.putAll(getTestOptionsMap());
    return properties;
  }

  private ImmutableMap getTestOptionsMap() {
    return new ImmutableMap.Builder<>()
        .put("project", "unit-test-project")
        .put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer")
        .put("value.deserializer",
            "org.apache.kafka.common.serialization.IntegerDeserializer")
        .put("max.poll.records", 500)
        .put("group.id", "groupId")
        .put("subscription.allow.create", false)
        .put("subscription.allow.delete", false)
        .build();
  }
}
