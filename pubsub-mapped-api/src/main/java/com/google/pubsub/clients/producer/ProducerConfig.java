// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////

package com.google.pubsub.clients.producer;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.ValidString;

/**
 * Provides the configurations for a KafkaProducer instance.
 */
public class ProducerConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String PROJECT_CONFIG = "project";
  private static final String PROJECT_DOC = "GCP project that we will connect to.";

  public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
  private static final String KEY_SERIALIZER_CLASS_DOC = "Serializer class for key that implements"
      + " the <code>Serializer</code> interface.";

  public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
  private static final String VALUE_SERIALIZER_CLASS_DOC = "Serializer class for value that implements"
      + " the <code>Serializer</code> interface.";

  public static final String ELEMENTS_COUNT_CONFIG = "element.count";
  private static final String ELEMENTS_COUNT_DOC = "This configuration controls the default count of"
      + " elements in a batch.";

  public static final String BATCH_SIZE_CONFIG = "batch.size";
  private static final String BATCH_SIZE_DOC = "This configuration controls the default batch size in bytes.";

  public static final String LINGER_MS_CONFIG = "linger.ms";
  private static final String LINGER_MS_DOC = "This setting adds a small amount of artificial delay."
      + "That is, rather than immediately sending out a record the producer will wait for up to "
      + "the given delay to allow other records to be sent so that the sends can be batched together.";

  public static final String ACKS_CONFIG = "acks";
  private static final String ACKS_DOC = "The number of acknowledgments the producer requires to"
      + " consider a request complete.";

  public static final String RETRIES_CONFIG = "retries";
  private static final String RETRIES_DOC = "Setting a value greater than zero will cause the client to"
      + " resend any record whose send fails with a potentially transient error.";

  public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
  private static final String RETRY_BACKOFF_MS_DOC = "The amount of time to wait before attempting to"
      + " retry a failed request to a given topic. This avoids repeatedly sending requests"
      + " in a tight loop under some failure scenarios.";

  public static final String AUTO_CREATE_CONFIG = "auto.create.topics.enable";
  private static final String AUTO_CREATE_DOC = "A flag, when true topics are automatically created"
      + " if they don't exist.";

  public static final String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";
  private static final String REQUEST_TIMEOUT_MS_DOC = "The configuration controls the maximum"
      + " amount of time the client will wait for the response of a request. If the response is not"
      + " received before the timeout elapses the client will resend the request if necessary or"
      + " fail the request if retries are exhausted.";

  public static final String CLIENT_ID_CONFIG = "client.id";
  private static final String CLIENT_ID_DOC = "An id string to pass to the server when making"
      + " requests. The purpose of this is to be able to track the source of requests beyond"
      + " just ip/port by allowing a logical application name to be included in server-side"
      + " request logging.";


  static {
    CONFIG = new ConfigDef()
        .define(PROJECT_CONFIG,
            Type.STRING,
            Importance.HIGH,
            PROJECT_DOC)
        .define(KEY_SERIALIZER_CLASS_CONFIG,
            Type.CLASS, Importance.HIGH,
            KEY_SERIALIZER_CLASS_DOC)
        .define(VALUE_SERIALIZER_CLASS_CONFIG,
            Type.CLASS,
            Importance.HIGH,
            VALUE_SERIALIZER_CLASS_DOC)
        .define(AUTO_CREATE_CONFIG,
            Type.BOOLEAN,
            true,
            Importance.MEDIUM,
            AUTO_CREATE_DOC)
        .define(ACKS_CONFIG,
            Type.STRING,
            "1",
            ValidString.in("-1", "0", "1", "all"),
            Importance.MEDIUM, ACKS_DOC)
        .define(LINGER_MS_CONFIG,
            Type.LONG,
            1,
            Range.atLeast(1L),
            Importance.MEDIUM, LINGER_MS_DOC)
        .define(BATCH_SIZE_CONFIG,
            Type.INT,
            16384,
            Range.atLeast(1),
            Importance.MEDIUM,
            BATCH_SIZE_DOC)
        .define(ELEMENTS_COUNT_CONFIG,
            Type.LONG,
            1000L,
            Range.atLeast(1L),
            Importance.MEDIUM,
            ELEMENTS_COUNT_DOC)
        .define(RETRIES_CONFIG,
            Type.INT,
            0,
            Range.atLeast(0),
            Importance.MEDIUM,
            RETRIES_DOC)
        .define(RETRY_BACKOFF_MS_CONFIG,
            Type.LONG,
            100L,
            Range.atLeast(0L),
            Importance.LOW,
            RETRY_BACKOFF_MS_DOC)
        .define(REQUEST_TIMEOUT_MS_CONFIG,
            Type.INT,
            30 * 1000,
            Range.atLeast(0),
            Importance.MEDIUM,
            REQUEST_TIMEOUT_MS_DOC)
        .define(CLIENT_ID_CONFIG,
            Type.STRING,
            "",
            Importance.MEDIUM,
            CLIENT_ID_DOC)
        ;
  }

  static Map<String, Object> addSerializerToConfig(Map<String, Object> configs,
      Serializer<?> keySerializer, Serializer<?> valueSerializer) {

    Map<String, Object> newConfigs = new HashMap<>();
    newConfigs.putAll(configs);

    if (keySerializer != null)
      newConfigs.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass());

    if (valueSerializer != null)
      newConfigs.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass());

    return newConfigs;
  }

  static Properties addSerializerToConfig(Properties properties,
      Serializer<?> keySerializer, Serializer<?> valueSerializer) {

    Properties newProperties = new Properties();
    newProperties.putAll(properties);

    if (keySerializer != null)
      newProperties.put(KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass().getName());

    if (valueSerializer != null)
      newProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass().getName());

    return newProperties;
  }

  ProducerConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }

  ProducerConfig(Map<?, ?> props, boolean doLog) {
    super(CONFIG, props, doLog);
  }

  public static Set<String> configNames() {
    return CONFIG.names();
  }

  public static void main(String[] args) {
    System.out.println(CONFIG.toHtmlTable());
  }

}