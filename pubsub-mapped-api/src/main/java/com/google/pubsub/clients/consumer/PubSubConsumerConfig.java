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

  /** <code>max.per.request.changes</code> */
  public static final String MAX_PER_REQUEST_CHANGES_CONFIG = "max.per.request.changes";
  private static final String MAX_PER_REQUEST_CHANGES_DOC =
      "Maximum of request changes sent in single message (acknowledge & extend deadline)";

  public static final String CREATED_SUBSCRIPTION_DEADLINE_SECONDS_CONFIG = "created.subscription.deadline.sec";
  private static final String CREATED_SUBSCRIPTION_DEADLINE_SECONDS_DOC =
      "If creation of subscriptions is configured, this is the acknowledge deadline they are going to get";


  public static final String MAX_ACK_EXTENSION_PERIOD_SECONDS_CONFIG = "max.ack.extension.period.sec";
  private static final String MAX_ACK_EXTENSION_PERIOD_SECONDS_DOC =
    "The maximum period a message ack deadline will be extended";

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
        .define(MAX_PER_REQUEST_CHANGES_CONFIG,
            Type.INT,
            1000,
            Importance.MEDIUM,
            MAX_PER_REQUEST_CHANGES_DOC)
        .define(CREATED_SUBSCRIPTION_DEADLINE_SECONDS_CONFIG,
            Type.INT,
            20,
            Importance.MEDIUM,
            CREATED_SUBSCRIPTION_DEADLINE_SECONDS_DOC)
        .define(MAX_ACK_EXTENSION_PERIOD_SECONDS_CONFIG,
            Type.INT,
            3600, // 60 minutes
            Importance.MEDIUM,
            MAX_ACK_EXTENSION_PERIOD_SECONDS_DOC);
  }

  PubSubConsumerConfig(Map<?, ?> props) {
    super(CONFIG, props);
  }

  PubSubConsumerConfig(Map<?, ?> props, boolean doLog) {
    super(CONFIG, props, doLog);
  }

}