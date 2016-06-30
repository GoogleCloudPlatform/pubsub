// Copyright 2016 Google Inc.
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
package com.google.pubsub.kafka.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link SourceConnector} that writes messages to a specific topic in Kafka.
 */
public class CloudPubSubSourceConnector extends SourceConnector {

  @Override
  public String version() {
    return null;
  }

  @Override
  public void start(Map<String, String> map) {

  }

  @Override
  public Class<? extends Task> taskClass() {
    return null;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    return null;
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return null;
  }
}

