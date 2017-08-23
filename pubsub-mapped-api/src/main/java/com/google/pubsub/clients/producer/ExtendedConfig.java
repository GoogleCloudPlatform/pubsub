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
import com.google.common.annotations.VisibleForTesting;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerConfigAdapter;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Importance;

/**
 * Provides the configurations for a Kafka Producer instance.
 */
@VisibleForTesting
public class ExtendedConfig {

  private ProducerConfig kafkaConfigs;
  private PubSubConfig additionalConfigs;

  public ExtendedConfig(Map configs) {
    additionalConfigs = new PubSubConfig(configs);
    kafkaConfigs = ProducerConfigAdapter.getConsumerConfig(configs);
  }

  public ProducerConfig getKafkaConfigs() {
    return kafkaConfigs;
  }

  public PubSubConfig getAdditionalConfigs() {
    return additionalConfigs;
  }

  public static class PubSubConfig extends AbstractConfig {

    public static final String PROJECT_CONFIG = "project";
    private static final String PROJECT_DOC = "GCP project that we will connect to.";

    public static final String ELEMENTS_COUNT_CONFIG = "element.count";
    private static final String ELEMENTS_COUNT_DOC = "This configuration controls the default count of"
            + " elements in a batch.";

    public static final String AUTO_CREATE_CONFIG = "auto.create.topics.enable";
    private static final String AUTO_CREATE_DOC = "A flag, when true topics are automatically created"
            + " if they don't exist.";


    private static final ConfigDef CONFIG = new ConfigDef()
            .define(PROJECT_CONFIG, Type.STRING, Importance.HIGH, PROJECT_DOC)
            .define(AUTO_CREATE_CONFIG, Type.BOOLEAN, true, Importance.MEDIUM, AUTO_CREATE_DOC)
            .define(ELEMENTS_COUNT_CONFIG, Type.LONG, 1000L, Range.atLeast(1L), Importance.MEDIUM, ELEMENTS_COUNT_DOC);

    PubSubConfig(Map<?, ?> originals, boolean doLog) {
      super(CONFIG, originals, doLog);
    }

    PubSubConfig(Map<?, ?> originals) {
      super(CONFIG, originals);
    }

  }
}