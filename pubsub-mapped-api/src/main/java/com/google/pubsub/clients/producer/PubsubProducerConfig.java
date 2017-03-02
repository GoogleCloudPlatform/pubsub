/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.pubsub.clients.producer;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.serialization.Serializer;

public class PubsubProducerConfig extends AbstractConfig {
  private static final ConfigDef CONFIG;

  public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
  private static final String KEY_SERIALIZER_CLASS_DOC = "Serializer class for key that implements the <code>Serializer</code> interface.";

  public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
  private static final String VALUE_SERIALIZER_CLASS_DOC = "Serializer class for vlaue that implements the <code>Serializer</code> interface.";

  public static final String BATCH_SIZE_CONFIG = "batch.size";
  private static final String BATCH_SIZE_DOC = "Batch size to use for publishing.";

  public static final String ACKS_CONFIG = "acks";
  private static final String ACKS_DOC = "Whether server acks are needed before a publish request completes.";

  public static final String PROJECT_CONFIG = "project";
  private static final String PROJECT_DOC = "GCP project to use with the publisher.";

  public static final String MAX_REQUEST_SIZE_CONFIG = "max.request.size";
  private static final String MAX_REQUEST_SIZE_DOC = "The maximum size of a request in bytes.";

  public static final int DEFAULT_BATCH_SIZE = 1;
  public static final boolean DEFAULT_ACKS = true;
  public static final int DEFAULT_MAX_REQUEST_SIZE = 1*1024*1024;

  static {
    CONFIG =
        new ConfigDef()
            .define(
                KEY_SERIALIZER_CLASS_CONFIG, Type.CLASS, Importance.HIGH, KEY_SERIALIZER_CLASS_DOC)
            .define(
                VALUE_SERIALIZER_CLASS_CONFIG, Type.CLASS, Importance.HIGH, VALUE_SERIALIZER_CLASS_DOC)
            .define(PROJECT_CONFIG, Type.STRING, Importance.HIGH, PROJECT_DOC)
            .define(BATCH_SIZE_CONFIG, Type.INT, DEFAULT_BATCH_SIZE, Importance.MEDIUM, BATCH_SIZE_DOC)
            .define(ACKS_CONFIG, Type.STRING, "1", Importance.MEDIUM, ACKS_DOC)
            .define(MAX_REQUEST_SIZE_CONFIG, Type.INT, DEFAULT_MAX_REQUEST_SIZE, atLeast(0), Importance.MEDIUM, MAX_REQUEST_SIZE_DOC);
  }

  PubsubProducerConfig(Map<?, ?> properties) {
    super(CONFIG, properties);
  }

  public static Map<String, Object> addSerializerToConfig(Map<String, Object> configs, Serializer<?> keySerializer, Serializer<?> valueSerializer) {
    Map<String, Object> newConfigs = new HashMap<String, Object>();
    newConfigs.putAll(configs);
    if (keySerializer != null) {
      newConfigs.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass());
    }
    if (valueSerializer != null) {
      newConfigs.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass());
    }
    return newConfigs;
  }

  public static Properties addSerializerToConfig(Properties properties, Serializer<?> keySerializer, Serializer<?> valueSerializer) {
    Properties newProperties = new Properties();
    newProperties.putAll(properties);
    if (keySerializer != null) {
      newProperties.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass());
    }
    if (valueSerializer != null) {
      newProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass());
    }
    return newProperties;
  }
}