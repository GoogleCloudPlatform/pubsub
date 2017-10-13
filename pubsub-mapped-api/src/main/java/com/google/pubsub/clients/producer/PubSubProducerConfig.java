package com.google.pubsub.clients.producer;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Importance;

/**
 * The Producer configuration keys
 */
public class PubSubProducerConfig extends AbstractConfig {

  /** <code>project</code> */
  public static final String PROJECT_CONFIG = "project";
  private static final String PROJECT_DOC = "GCP project that we will connect to.";

  /** <code>elements.count</code> */
  public static final String ELEMENTS_COUNT_CONFIG = "elements.count";
  private static final String ELEMENTS_COUNT_DOC = "This configuration controls the default count of"
      + " elements in a batch.";

  /** <code>auto.create.topics.enable</code> */
  public static final String AUTO_CREATE_TOPICS_CONFIG = "auto.create.topics.enable";
  private static final String AUTO_CREATE_TOPICS_DOC = "When true topics are automatically created"
      + " if they don't exist.";

  private static final ConfigDef CONFIG = new ConfigDef()
      .define(PROJECT_CONFIG,
          Type.STRING,
          Importance.HIGH,
          PROJECT_DOC)
      .define(AUTO_CREATE_TOPICS_CONFIG,
          Type.BOOLEAN,
          true,
          Importance.MEDIUM,
          AUTO_CREATE_TOPICS_DOC)
      .define(ELEMENTS_COUNT_CONFIG,
          Type.LONG,
          1000L,
          Range.atLeast(1L),
          Importance.MEDIUM,
          ELEMENTS_COUNT_DOC);

  PubSubProducerConfig(Map<?, ?> originals, boolean doLog) {
    super(CONFIG, originals, doLog);
  }

  PubSubProducerConfig(Map<?, ?> originals) {
    super(CONFIG, originals);
  }
}
