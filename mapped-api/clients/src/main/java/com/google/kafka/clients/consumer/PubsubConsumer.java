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

  }
=======
package com.google.kafka.cients.consumer;

public class PubsubConsumer<K, V> implements Consumer<K, V> {

    private static final Logger log = LoggerFactor.getLogger(PubsubConsumer.class);
    private static final long NO_CURRENT_THREAD = -1L;
    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final String JMX_PREFIX = "pubsub.consumer";
    static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;

    private final String clientId;
    private final ConsumerCoordinator coordinator;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final Fetcher<K, V> fetcher;
    private final ConsumerInterceptors<K, V> interceptors;

    private final Time time;
    private final ConsumerNetworkClient client;
    private final Metrics metrics;
    private final SubscriptionState subscriptions;
    private final Metadata metadata;    // not sure if pubsub will have metadata
    private final long retryBackoffMs;
    private final long requestTimeoutMs;
    private volatile boolean closed = false;

    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
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
            log.debug("Starting the Kafka consumer");
            this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            int sessionTimeoutMs = config.getInt(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
            int fetchMaxWaitMs = config.getInt(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
            if (this.requestTimeoutMs <= sessionTimeoutMs || this.requestTimeoutMs <= fetchMaxWaitMs)
                throw new ConfigException(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG + " should greater than " + ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG + " and " + ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG);
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
            List<MetricsReporters> reporters = config.getConfiguredInstances(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
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
               // this.valueDeserializer = config.getConfigured
            }
        }   // left off at kafka's line 645
    }

    /**
     * Get the set of partitions currently assigned to this consumer. If subscription happened by directly assigning
     * partitions using {@link #assign(Collection)} then this will simply return the same partitions that
     * were assigned. If topic subscription was used, then this will give the set of topic partitions currently assigned
     * to the consumer (which may be none if the assignment hasn't happened yet, or the partitions are in the
     * process of getting reassigned).
     * @return The set of partitions currently assigned to this consumer
     */
    public Set<TopicPartition> assignment() {

    }

    /**
     * Get the current subscription. Will return the same topics used in the most recent call to
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, or an empty set if no such call has been made.
     * @return The set of topics currently subscribed to
     */
    public Set<String> subscription() {

    }

    /**
     * Subscribe to the given list of topics to get dynamically
     * assigned partitions. <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> Note that it is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     *
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that belong to a particular
     * group and will trigger a rebalance operation if one of the following events trigger -
     * <ul>
     * <li>Number of partitions change for any of the subscribed list of topics
     * <li>Topic is created or deleted
     * <li>An existing member of the consumer group dies
     * <li>A new member is added to an existing consumer group via the join API
     * </ul>
     * <p>
     * When any of these events are triggered, the provided listener will be invoked first to indicate that
     * the consumer's assignment has been revoked, and then again when the new assignment has been received.
     * Note that this listener will immediately override any listener set in a previous call to subscribe.
     * It is guaranteed, however, that the partitions revoked/assigned through this interface are from topics
     * subscribed in this call. See {@link ConsumerRebalanceListener} for more details.
     *
     * @param topics The list of topics to subscribe to
     * @param listener Non-null listener instance to get notifications on partition assignment/revocation for the
     *                 subscribed topics
     * @throws IllegalArgumentException If topics is null or contains null or empty elements
     */
    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {

    }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     * <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> It is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     *
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * This is a short-hand for {@link #subscribe(Collection, ConsumerRebalanceListener)}, which
     * uses a noop listener. If you need the ability to seek to particular offsets, you should prefer
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, since group rebalances will cause partition offsets
     * to be reset. You should also provide your own listener if you are doing your own offset
     * management since the listener gives you an opportunity to commit offsets before a rebalance finishes.
     *
     * @param topics The list of topics to subscribe to
     * @throws IllegalArgumentException If topics is null or contains null or empty elements
     */
    @Override
    public void subscribe(Collection<String> topics) {

    }

    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions. The pattern matching will be done periodically against topics
     * existing at the time of check.
     *
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that
     * belong to a particular group and will trigger a rebalance operation if one of the
     * following events trigger -
     * <ul>
     * <li>Number of partitions change for any of the subscribed list of topics
     * <li>Topic is created or deleted
     * <li>An existing member of the consumer group dies
     * <li>A new member is added to an existing consumer group via the join API
     * </ul>
     *
     * @param pattern Pattern to subscribe to
     * @param listener Non-null listener instance to get notifications on partition assignment/revocation for the
     *                 subscribed topics
     * @throws IllegalArgumentException If pattern is null
     */
    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {

    }

    /**
     * Unsubscribe from topics currently subscribed with {@link #subscribe(Collection)}. This
     * also clears any partitions directly assigned through {@link #assign(Collection)}.
     */
     public void unsubscribe() {

     }

    /**
     * Manually assign a list of partitions to this consumer. This interface does not allow for incremental assignment
     * and will replace the previous assignment (if there is one).
     *
     * If the given list of topic partitions is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * Manual topic assignment through this method does not use the consumer's group management
     * functionality. As such, there will be no rebalance operation triggered when group membership or cluster and topic
     * metadata change. Note that it is not possible to use both manual partition assignment with {@link #assign(Collection)}
     * and group assignment with {@link #subscribe(Collection, ConsumerRebalanceListener)}.
     *
     * @param partitions The list of partitions to assign this consumer
     * @throws IllegalArgumentException If partitions is null or contains null or empty topics
     */
    @Override
    public void assign(Collection<TopicPartition> partitions) {

    }


    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It is an error to not have
     * subscribed to any topics or partitions before polling for data.
     * <p>
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially. The last
     * consumed offset can be manually set through {@link #seek(TopicPartition, long)} or automatically set as the last committed
     * offset for the subscribed list of partitions
     *
     *
     * @param timeout The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
     *            If 0, returns immediately with any records that are available currently in the buffer, else returns empty.
     *            Must not be negative.
     * @return map of topic to records since the last fetch for the subscribed list of topics and partitions
     *
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if the offset for a partition or set of
     *             partitions is undefined or out of range and no offset reset policy has been configured
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthorizationException if caller lacks Read access to any of the subscribed
     *             topics or to the configured groupId
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. invalid groupId or
     *             session timeout, errors deserializing key/value pairs, or any new error cases in future versions)
     * @throws java.lang.IllegalArgumentException if the timeout value is negative
     * @throws java.lang.IllegalStateException if the consumer is not subscribed to any topics or manually assigned any
     *             partitions to consume from
     */
    @Override
    public ConsumerRecords<K, V> poll(long timeout) {

    }

    /**
     * Commit offsets returned on the last {@link #poll(long) poll()} for all the subscribed list of topics and partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is a synchronous commits and will block until either the commit succeeds or an unrecoverable error is
     * encountered (in which case it is thrown to the caller).
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same groupId which is using group management.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the committed offset is invalid).
     */
    @Override
    public void commitSync() {

    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1.
     * <p>
     * This is a synchronous commits and will block until either the commit succeeds or an unrecoverable error is
     * encountered (in which case it is thrown to the caller).
     *
     * @param offsets A map of offsets by partition with associated metadata
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried.
     *             This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *             or if there is an active group with the same groupId which is using group management.
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. if offset metadata
     *             is too large or if the committed offset is invalid).
     */
    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    /**
     * Commit offsets returned on the last {@link #poll(long) poll()} for all the subscribed list of topics and partition.
     * Same as {@link #commitAsync(OffsetCommitCallback) commitAsync(null)}
     */
    @Override
    public void commitAsync() {

    }

    /**
     * Commit offsets returned on the last {@link #poll(long) poll()} for the subscribed list of topics and partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     *
     * @param callback Callback to invoke when the commit completes
     */
    @Override
    public void commitAsync(OffsetCommitCallback callback) {

    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions to Kafka.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     *
     * @param offsets A map of offsets by partition with associate metadata. This map will be copied internally, so it
     *                is safe to mutate the map after returning.
     * @param callback Callback to invoke when the commit completes
     */
    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {

    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(long) poll(timeout)}. If this API
     * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets
     */
    @Override
    public void seek(TopicPartition partition, long offset) {

    }

    /**
     * Seek to the first offset for each of the given partitions. This function evaluates lazily, seeking to the
     * first offset in all partitions only when {@link #poll(long)} or {@link #position(TopicPartition)} are called.
     * If no partition is provided, seek to the first offset for all of the currently assigned partitions.
     */
    public void seekToBeginning(Collection<TopicPartition> partitions) {

    }

    /**
     * Seek to the last offset for each of the given partitions. This function evaluates lazily, seeking to the
     * final offset in all partitions only when {@link #poll(long)} or {@link #position(TopicPartition)} are called.
     * If no partition is provided, seek to the final offset for all of the currently assigned partitions.
     */
    public void seekToEnd(Collection<TopicPartition> partitions) {

    }

    /**
     * Get the offset of the <i>next record</i> that will be fetched (if a record with that offset exists).
     *
     * @param partition The partition to get the position for
     * @return The offset
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if no offset is currently defined for
     *             the partition
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    public long position(TopicPartition partition) {

    }

    /**
     * Get the last committed offset for the given partition (whether the commit happened by this process or
     * another). This offset will be used as the position for the consumer in the event of a failure.
     * <p>
     * This call may block to do a remote call if the partition in question isn't assigned to this consumer or if the
     * consumer hasn't yet initialized its cache of committed offsets.
     *
     * @param partition The partition to check
     * @return The last committed offset and metadata or null if there was no prior commit
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the topic or to the
     *             configured groupId
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {

    }

    /**
     * Get the metrics kept by the consumer
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Get metadata about the partitions for a given topic. This method will issue a remote call to the server if it
     * does not already have any metadata about the given topic.
     *
     * @param topic The topic to get partition metadata for
     * @return The list of partitions
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the specified topic
     * @throws org.apache.kafka.common.errors.TimeoutException if the topic metadata could not be fetched before
     *             expiration of the configured request timeout
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {

    }

    /**
     * Get metadata about partitions for all topics that the user is authorized to view. This method will issue a
     * remote call to the server.
     * @return The map of topics and its partitions
     * @throws org.apache.kafka.common.errors.WakeupException if {@link #wakeup()} is called before or while this
     *             function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *             this function is called
     * @throws org.apache.kafka.common.errors.TimeoutException if the topic metadata could not be fetched before
     *             expiration of the configured request timeout
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics() {

    }

    /**
     * Suspend fetching from the requested partitions. Future calls to {@link #poll(long)} will not return
     * any records from these partitions until they have been resumed using {@link #resume(Collection)}.
     * Note that this method does not affect partition subscription. In particular, it does not cause a group
     * rebalance when automatic assignment is used.
     * @param partitions The partitions which should be paused
     */
    @Override
    public void pause(Collection<TopicPartition> partitions) {

    }

    /**
     * Resume specified partitions which have been paused with {@link #pause(Collection)}. New calls to
     * {@link #poll(long)} will return records from these partitions if there are any to be fetched.
     * If the partitions were not previously paused, this method is a no-op.
     * @param partitions The partitions which should be resumed
     */
    @Override
    public void resume(Collection<TopicPartition> partitions) {

    }

    /**
     * Get the set of partitions that were previously paused by a call to {@link #pause(Collection)}.
     *
     * @return The set of paused partitions
     */
    @Override
    public Set<TopicPartition> paused() {

    }

    /**
     * Look up the offsets for the given partitions by timestamp. The returned offset for each partition is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
     *
     * This is a blocking call. The consumer does not have to be assigned the partitions.
     * If the message format version in a partition is before 0.10.0, i.e. the messages do not have timestamps, null
     * will be returned for that partition.
     *
     * Notice that this method may block indefinitely if the partition does not exist.
     *
     * @param timestampsToSearch the mapping from partition to the timestamp to look up.
     * @return a mapping from partition to the timestamp and offset of the first message with timestamp greater
     *         than or equal to the target timestamp. {@code null} will be returned for the partition if there is no
     *         such message.
     * @throws IllegalArgumentException if the target timestamp is negative.
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the broker does not support looking up
     *         the offsets by timestamp.
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {

    }

    /**
     * Get the first offset for the given partitions.
     * <p>
     * Notice that this method may block indefinitely if the partition does not exist.
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToBeginning(Collection)
     *
     * @param partitions the partitions to get the earliest offsets.
     * @return The earliest available offsets for the given partitions
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {

    }

    /**
     * Get the last offset for the given partitions. The last offset of a partition is the offset of the upcoming
     * message, i.e. the offset of the last available message + 1.
     * <p>
     * Notice that this method may block indefinitely if the partition does not exist.
     * This method does not change the current consumer position of the partitions.
     *
     * @see #seekToEnd(Collection)
     *
     * @param partitions the partitions to get the end offsets.
     * @return The end offsets for the given partitions.
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {

    }

    /**
     * Close the consumer, waiting for up to the default timeout of 30 seconds for any needed cleanup.
     * If auto-commit is enabled, this will commit the current offsets if possible within the default
     * timeout. See {@link #close(long, TimeUnit)} for details. Note that {@link #wakeup()}
     * cannot be used to interrupt close.
     *
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted
     * before or while this function is called
     */
    @Override
    public void close() {

    }

    /**
     * Tries to close the consumer cleanly within the specified timeout. This method waits up to
     * <code>timeout</code> for the consumer to complete pending commits and leave the group.
     * If auto-commit is enabled, this will commit the current offsets if possible within the
     * timeout. If the consumer is unable to complete offset commits and gracefully leave the group
     * before the timeout expires, the consumer is force closed. Note that {@link #wakeup()} cannot be
     * used to interrupt close.
     *
     * @param timeout The maximum time to wait for consumer to close gracefully. The value must be
     *                non-negative. Specifying a timeout of zero means do not wait for pending requests to complete.
     * @param timeUnit The time unit for the <code>timeout</code>
     * @throws InterruptException If the thread is interrupted before or while this function is called
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     */
    public void close(long timeout, TimeUnit timeUnit) {

    }

    /**
     * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long poll.
     * The thread which is blocking in an operation will throw {@link org.apache.kafka.common.errors.WakeupException}.
     * If no thread is blocking in a method which can throw {@link org.apache.kafka.common.errors.WakeupException}, the next call to such a method will raise it instead.
     */
    @Override
    public void wakeup() {

    }
}