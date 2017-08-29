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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

//TODO Use builder pattern using Kafka's ConsumerConfig class
/**
 * The consumer configuration keys
 */
public class PubSubConsumerConfig extends AbstractConfig {
  private static final ConfigDef CONFIG = getInstance();

  /** <code>subscription.allow.create</code> */
  public static final String SUBSCRIPTION_ALLOW_CREATE_CONFIG = "subscription.allow.create";
  private static final String SUBSCRIPTION_ALLOW_CREATE_DOC =
      "Determines if subscriptions for non-existing groups should be created";

  /** <code>subscription.allow.delete</code> */
  public static final String SUBSCRIPTION_ALLOW_DELETE_CONFIG = "subscription.allow.delete";
  private static final String SUBSCRIPTION_ALLOW_DELETE_DOC =
      "Determines if subscriptions for non-existing groups should be created";

  /** <code>ack.expiration.millis</code> */
  public static final String ACK_EXPIRATION_MILLIS_CONFIG = "ack.expiration.millis";
  private static final String ACK_EXPIRATION_MILLIS_DOC =
      "Acknowledge deadline for PubSub";

  private static synchronized ConfigDef getInstance() {
    return new ConfigDef()
        .define(SUBSCRIPTION_ALLOW_CREATE_CONFIG,
            Type.BOOLEAN,
            false,
            Importance.MEDIUM,
            SUBSCRIPTION_ALLOW_CREATE_DOC)
        .define(SUBSCRIPTION_ALLOW_DELETE_CONFIG,
            Type.BOOLEAN,
            false,
            Importance.MEDIUM,
            SUBSCRIPTION_ALLOW_DELETE_DOC)
        .define(ACK_EXPIRATION_MILLIS_CONFIG,
            Type.INT,
            600000,
            Importance.MEDIUM,
            ACK_EXPIRATION_MILLIS_DOC);
  }

  PubSubConsumerConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }

  PubSubConsumerConfig(Map<?, ?> props, boolean doLog) {
    super(CONFIG, props, doLog);
  }

}