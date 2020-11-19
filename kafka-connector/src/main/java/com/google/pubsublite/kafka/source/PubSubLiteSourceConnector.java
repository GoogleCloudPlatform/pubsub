package com.google.pubsublite.kafka.source;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public class PubSubLiteSourceConnector extends SourceConnector {

  private Map<String, String> props;

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    props = map;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return PubSubLiteSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    return Collections.nCopies(i, props);
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return ConfigDefs.config();
  }
}
