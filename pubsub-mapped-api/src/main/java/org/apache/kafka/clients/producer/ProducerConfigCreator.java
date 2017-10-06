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

package org.apache.kafka.clients.producer;

import java.util.Map;
import java.util.Properties;

/**
 * This is an adapter to control the ProducerConfig class since it's constructors are package-private.
 */
public class ProducerConfigAdapter {
  
  public static ProducerConfig getProducerConfig(Properties properties) {
    return getProducerConfig(properties);
  }

  public static ProducerConfig getProducerConfig(Map<String, Object> configurations) {
    addDefaultKafkaRequiredConfigs(configurations);
    return new ProducerConfig(configurations);
  }

  private static void addDefaultKafkaRequiredConfigs(Map map) {
    if (!map.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    }

    if (!map.containsKey(ProducerConfig.LINGER_MS_CONFIG)
        || Integer.parseInt((String) map.get(ProducerConfig.LINGER_MS_CONFIG)) == 0) {
      map.put(ProducerConfig.LINGER_MS_CONFIG, "1");
    }
  }
}