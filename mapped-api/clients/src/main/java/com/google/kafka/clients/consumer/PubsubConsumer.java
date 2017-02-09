/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.google.kafka.clients.consumer;

public class PubsubConsumer<K, V> implements Consumer<K, V> {

  private static final Logger log = LoggerFactory.getLogger(PubsubConsumer.class);
  private static final long NO_CURRENT_THREAD = -1L;
  private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
  private static final String JMX_PREFIX = "cps.consumer";
  static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;

  private final String clientId;
  private final ConsumerCoordinator coordinator;  // this might need to be an /internal class
  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;
  private final Fetcher<K, V> fetcher;
  private final ConsumerInterceptors<K, V> interceptors;

  private final Time time;
  private final ConsumerNetworkClient client;
  private final Metrics metrics;
  private final SubscriptionState subscriptions;
  private final long retryBackoffMs;
  private final long requestTimeoutMs;
  private volatile boolean closed = false;

  // currentThread holds the threadId of the current thread accessing PubsubConsumer
  // and is used to prevent multi-threaded access
  private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
  // refcount is used to allow reentrant access by the thread who has acquired currentThread
  private final AtomicInteger refcount = new AtomicInteger(0);

  /**
   * A consumer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
   * are documented <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >here</a>. Values can be
   * either strings or objects of the appropriate type (for example a numeric configuration would accept either the
   * string "42" or the integer 42).
   * <p>
   * Valid configuration strings are documented at {@link ConsumerConfig}
   *
   * @param configs The consumer configs
   */
  public PubsubConsumer(Map<String, Object> configs) {
    this(configs, null, null);
  }

  /**
   * A consumer is instantiated by providing a set of key-value pairs as configuration, and a key and a value {@link Deserializer}.
   * <p>
   * Valid configuration strings are documented at {@link ConsumerConfig}
   *
   * @param configs The consumer configs
   * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
   *            won't be called in the consumer when the deserializer is passed in directly.
   * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
   *            won't be called in the consumer when the deserializer is passed in directly.
   */
  public PubsubConsumer(Map<String, Object> configs,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    this(new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
        keyDeserializer,
        valueDeserializer);
  }

  /**
   * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration.
   * <p>
   * Valid configuration strings are documented at {@link ConsumerConfig}
   *
   * @param properties The consumer configuration properties
   */
  public PubsubConsumer(Properties properties) {
    this(properties, null, null);
  }

  /**
   * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration, and a
   * key and a value {@link Deserializer}.
   * <p>
   * Valid configuration strings are documented at {@link ConsumerConfig}
   *
   * @param properties The consumer configuration properties
   * @param keyDeserializer The deserializer for key that implements {@link Deserializer}. The configure() method
   *            won't be called in the consumer when the deserializer is passed in directly.
   * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
   *            won't be called in the consumer when the deserializer is passed in directly.
   */
  public PubsubConsumer(Properties properties,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    this(new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(properties, keyDeserializer, valueDeserializer)),
        keyDeserializer,
        valueDeserializer);
  }

  @SuppressWarnings("unchecked")
  private PubsubConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
    try {
      log.debug("Starting the Pubsub consumer");
      this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
      int sessionTimeoutMs = config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
      int fetchMaxWaitMs = config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
      if (this.requestTimeoutMs <= sessionTimeOutMs || this.requestTimeoutMs <= fetchMaxWaitMs)
        throw new ConfigException(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG + " should be greater than " + ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG + " and " + ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
      this.time = Time.SYSTEM;

      String clientId = config.getString(ConsumerConfig.CLIENT_ID_CONFIG);
      if (clientId.length() <= 0)
        clientId = "consumer-" + CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement();
      this.clientId = clientId;
      Map<String, String> metricsTags = new LinkedHashMap<>();
      metricsTags.put("client-id", clientId);
      MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG))
          .timeWindow(config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
          .tags(metricsTags);
      List<MetricsReporter> reporters = config.getConfiguredInstances(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
          MetricsReporter.class);
      reporters.add(new JmxReporter(JMX_PREFIX));
      this.metrics = new Metrics(metricConfig, reporters, time);
      this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

      // load interceptors and make sure they get clientId
      Map<String, Object> userProvidedConfigs = config.originals();
      userProvidedConfigs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
      List<ConsumerInterceptor<K, V>> interceptorList = (List) (new ConsumerConfig(userProvidedConfigs)).getConfiguredInstances(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
          ConsumerInterceptor.class);
      this.interceptors = interceptorList.isEmpty() ? null : new ConsumerInterceptors<>(interceptorList);
      if (keyDeserializer == null) {
        this.keyDeserializer = config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            Deserializer.class);
        this.keyDeserializer.configure(config.originals(), true);
      } else {
        config.ignore(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        this.keyDeserializer = keyDeserializer;
      }
      if (valueDeserializer == null) {
        this.valueDeserializer = config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            Deserializer.class);
        this.valueDeserializer.configure(config.originals(), false);
      } else {
        config.ignore(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        this.valueDeserializer = valueDeserializer;
      }

    }
  }
}