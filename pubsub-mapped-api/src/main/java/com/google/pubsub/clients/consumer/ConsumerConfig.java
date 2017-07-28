/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.pubsub.clients.consumer;

import com.google.pubsub.kafkastubs.common.config.AbstractConfig;
import com.google.pubsub.kafkastubs.common.config.ConfigDef;
import com.google.pubsub.kafkastubs.common.config.ConfigDef.Importance;
import com.google.pubsub.kafkastubs.common.config.ConfigDef.Type;
import com.google.pubsub.kafkastubs.common.serialization.Deserializer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * The consumer configuration keys
 */
public class ConsumerConfig extends AbstractConfig {
  private static final ConfigDef CONFIG;

    /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS
     * THESE ARE PART OF THE PUBLIC API AND CHANGE WILL BREAK USER CODE.
     */
  /** <code>max.poll.records</code> */
  public static final String MAX_POLL_RECORDS_CONFIG = "max.poll.records";
  private static final String MAX_POLL_RECORDS_DOC = "The maximum number of records returned in a single call to poll().";

  /** <code>key.deserializer</code> */
  public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";
  public static final String KEY_DESERIALIZER_CLASS_DOC = "Deserializer class for key that implements the <code>Deserializer</code> interface.";

  /** <code>value.deserializer</code> */
  public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";
  public static final String VALUE_DESERIALIZER_CLASS_DOC = "Deserializer class for value that implements the <code>Deserializer</code> interface.";

  static {
    CONFIG = new ConfigDef()
        .define(MAX_POLL_RECORDS_CONFIG,
            Type.INT,
            Importance.MEDIUM,
            MAX_POLL_RECORDS_DOC)
        .define(KEY_DESERIALIZER_CLASS_CONFIG,
            Type.CLASS,
            Importance.HIGH,
            KEY_DESERIALIZER_CLASS_DOC)
        .define(VALUE_DESERIALIZER_CLASS_CONFIG,
            Type.CLASS,
            Importance.HIGH,
            VALUE_DESERIALIZER_CLASS_DOC);

  }

  public static Map<String, Object> addDeserializerToConfig(Map<String, Object> configs,
      Deserializer<?> keyDeserializer,
      Deserializer<?> valueDeserializer) {
    Map<String, Object> newConfigs = new HashMap<String, Object>();
    newConfigs.putAll(configs);
    if (keyDeserializer != null)
      newConfigs.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
    if (valueDeserializer != null)
      newConfigs.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
    return newConfigs;
  }

  public static Properties addDeserializerToConfig(Properties properties,
      Deserializer<?> keyDeserializer,
      Deserializer<?> valueDeserializer) {
    Properties newProperties = new Properties();
    newProperties.putAll(properties);
    if (keyDeserializer != null)
      newProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
    if (valueDeserializer != null)
      newProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());
    return newProperties;
  }

  ConsumerConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }

  ConsumerConfig(Map<?, ?> props, boolean doLog) {
    super(CONFIG, props, doLog);
  }

  public static Set<String> configNames() {
    return CONFIG.names();
  }

  public static void main(String[] args) {
    System.out.println(CONFIG.toHtmlTable());
  }

}