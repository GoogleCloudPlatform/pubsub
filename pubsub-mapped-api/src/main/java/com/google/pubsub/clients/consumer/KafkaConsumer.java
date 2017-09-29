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

import com.google.api.client.util.Base64;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.pubsub.clients.consumer.ack.MappedApiMessageReceiver;
import com.google.pubsub.clients.consumer.ack.Subscriber;
import com.google.pubsub.common.ChannelUtil;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherFutureStub;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumer<K, V> implements Consumer<K, V> {

  private static final ConsumerRebalanceListener NO_REBALANCE_LISTENER = null;
  private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
  private static final String GOOGLE_CLOUD_PROJECT = "projects/" +
      System.getenv("GOOGLE_CLOUD_PROJECT");
  private static final String SUBSCRIPTION_PREFIX = GOOGLE_CLOUD_PROJECT + "/subscriptions/";
  private static final String TOPIC_PREFIX = GOOGLE_CLOUD_PROJECT + "/topics/";

  //because of no partition concept, the partition number is constant
  private static final int DEFAULT_PARTITION = 0;

  //because checksum is not needed, it is defined as a constant
  private static final int DEFAULT_CHECKSUM = 1;

  private static final String KEY_ATTRIBUTE = "key";

  private final Config<K, V> config;
  private final SubscriberFutureStub subscriberFutureStub;
  private final PublisherFutureStub publisherFutureStub;

  private ImmutableList<String> topicNames = ImmutableList.of();

  private ImmutableMap<String, Subscriber> topicNameToSubscriber = ImmutableMap.of();

  private int currentPoolIndex;

  public KafkaConsumer(Map<String, Object> configs) {
    this(new Config<>(configs));
  }

  public KafkaConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    this(new Config<>(configs, keyDeserializer, valueDeserializer));
  }

  public KafkaConsumer(Properties properties) {
    this(new Config<>(properties));
  }

  public KafkaConsumer(Properties properties, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    this(new Config<>(properties, keyDeserializer, valueDeserializer));
  }


  private KafkaConsumer(Config configOptions) {
    this(configOptions,
        ChannelUtil.getInstance().getChannel(),
        ChannelUtil.getInstance().getCallCredentials());
  }

  @SuppressWarnings("unchecked")
  KafkaConsumer(Config config, Channel channel, CallCredentials callCredentials) {
    try {
      log.debug("Starting PubSub subscriber");

      Preconditions.checkNotNull(channel);

      SubscriberFutureStub subscriberFutureStub = SubscriberGrpc.newFutureStub(channel);
      PublisherFutureStub publisherFutureStub = PublisherGrpc.newFutureStub(channel);

      if(callCredentials != null) {
        subscriberFutureStub = subscriberFutureStub.withCallCredentials(callCredentials);
        publisherFutureStub = publisherFutureStub.withCallCredentials(callCredentials);
      }

      this.subscriberFutureStub = subscriberFutureStub;
      this.publisherFutureStub = publisherFutureStub;

      this.config = config;

      log.debug("PubSub subscriber created");
    } catch (Throwable t) {
      throw new KafkaException("Failed to construct PubSub subscriber", t);
    }
  }

  /**
   * Assignment returns a set of the topics it is subscribed to.
   * Set contains every topic once, combined with partition number 0 (default value).
   */
  @Override
  public Set<TopicPartition> assignment() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Returns set of topic names the consumer is subscribed to.
   */
  @Override
  public Set<String> subscription() {
    return topicNameToSubscriber.keySet();
  }

  /**
   * Subscribe call tries to get existing subscriptions for groupId provided in configuration and topic names.
   * Subscription names are created with pattern topicName_groupId.
   * If consumer was configured to create subscriptions if they don't exist, on failure with NOT_FOUND status it sends
   * "createSubscription" gRPC calls. If it was not configured to create subscriptions, throws KafkaException
   * if matching subscriptions do not exist.
   */
  @Override
  public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
    checkSubscribePreconditions(topics);

    unsubscribe();
    List<ResponseData<Subscription>> futureSubscriptions = deputePubsubSubscribesGet(topics);
    Map<String, Subscription> subscriptionMap = getSubscriptionsFromPubsub(futureSubscriptions);
    Map<String, Subscriber> tempSubscribersMap = new HashMap<>();

    for(Map.Entry<String, Subscription> entry: subscriptionMap.entrySet()) {
      Subscriber subscriber = getSubscriberFromConfigs(entry);

      tempSubscribersMap.put(entry.getKey(), subscriber);
      subscriber.startAsync().awaitRunning();
    }

    topicNameToSubscriber = ImmutableMap.copyOf(tempSubscribersMap);
    topicNames = ImmutableList.copyOf(subscriptionMap.keySet());
    currentPoolIndex = 0;

    log.debug("Subscribed to topic(s): {}", Utils.join(topics, ", "));
  }

  private Subscriber getSubscriberFromConfigs(Entry<String, Subscription> entry) {
    return Subscriber.defaultBuilder(entry.getValue(),
            new MappedApiMessageReceiver())
            .setFlowControlSettings(FlowControlSettings.getDefaultInstance())
            .setAutoCommit(config.getEnableAutoCommit())
            .setAutoCommitIntervalMs(config.getAutoCommitIntervalMs())
            .setMaxPullRecords((long) config.getMaxPollRecords())
            .setSubscriberFutureStub(this.subscriberFutureStub)
            .setRetryBackoffMs(config.getRetryBackoffMs())
            .setMaxAckExtensionPeriod(config.getMaxAckExtensionPeriod())
            .setMaxPerRequestChanges(config.getMaxPerRequestChanges())
            .setAckRequestTimeoutMs(config.getRequestTimeoutMs())
            .build();
  }

  private List<ResponseData<Subscription>> deputePubsubSubscribesGet(Collection<String> topics) {
    List<ResponseData<Subscription>> responseDatas = new ArrayList<>();
    Set<String> usedNames = new HashSet<>();

    for (String topic: topics) {
      if (!usedNames.contains(topic)) {
        String subscriptionString = SUBSCRIPTION_PREFIX + topic + "_" + config.getGroupId();
        ListenableFuture<Subscription> deputedSubscription =
            deputeSinglePubsubSubscriptionGet(subscriptionString);

        responseDatas.add(new ResponseData<>(topic, subscriptionString, deputedSubscription));
        usedNames.add(topic);
      }
    }
    return responseDatas;
  }

  private ListenableFuture<Subscription> deputeSinglePubsubSubscriptionGet(String subscriptionString) {
    return subscriberFutureStub
        .getSubscription(GetSubscriptionRequest.newBuilder()
            .setSubscription(subscriptionString)
            .build());
  }

  private Map<String, Subscription> getSubscriptionsFromPubsub(
      List<ResponseData<Subscription>> responseDatas) {
    Map<String, Subscription> subscriptionMap = new HashMap<>();

    for (ResponseData<Subscription> responseData: responseDatas) {
      boolean success = false;
      try {
        Subscription s = responseData.getRequestListenableFuture().get();
        subscriptionMap.put(responseData.getTopicName(), s);
        success = true;

      } catch (ExecutionException e) {

        if(!shouldTryToCreateSubscription(e)) {
          throw new KafkaException(e);
        }
        //TODO send all needed creation tasks at once rather than block on each of them (future)
        Subscription s = tryToCreareSubscription(responseData);
        subscriptionMap.put(responseData.getTopicName(), s);
        success = true;

      } catch (InterruptedException e) {
        throw new InterruptException(e);
      } finally {
        //if an error is thrown, attempt to delete subscriptions created in this loop
        if (!success)
          deleteSubscriptionsIfAllowed(subscriptionMap.values());
      }
    }
    return subscriptionMap;
  }

  private Subscription tryToCreareSubscription(ResponseData<Subscription> responseData) {
    try {
      ListenableFuture<Subscription> subscription = deputeSinglePubsubSubscription(
          responseData.getSubscriptionFullName(), responseData.getTopicName());
      return subscription.get();
    } catch (InterruptedException e) {
      throw new InterruptException(e);
    } catch (ExecutionException e) {
      throw new KafkaException(e);
    }
  }

  private boolean shouldTryToCreateSubscription(ExecutionException e) {
    return config.getAllowSubscriptionCreation() && e.getCause() instanceof StatusRuntimeException
        && ((StatusRuntimeException)e.getCause()).getStatus().getCode().equals(Code.NOT_FOUND);
  }

  private ListenableFuture<Subscription> deputeSinglePubsubSubscription(String subscriptionString,
      String topicName) {
    return subscriberFutureStub
        .createSubscription(Subscription.newBuilder()
            .setName(subscriptionString)
            .setTopic(TOPIC_PREFIX + topicName)
            .setAckDeadlineSeconds(config.getCreatedSubscriptionDeadlineSeconds())
            .build());
  }

  private void checkSubscribePreconditions(Collection<String> topics) {
    Preconditions.checkArgument(topics != null,
        "Topic collection to subscribe to cannot be null");

    for (String topic: topics) {
      Preconditions.checkArgument(topic != null && !topic.trim().isEmpty(),
          "Topic collection to subscribe to cannot contain null or empty topic");
    }
  }

  /**
   * Subscribes to the collection of topics
   * @param topics
   */
  @Override
  public void subscribe(Collection<String> topics) {
    subscribe(topics, NO_REBALANCE_LISTENER);
  }

  /**
  This version of subscribe pulls all topics from PubSub project and subscribes to ones matching provided
  pattern.
   */
  @Override
  public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
    checkPatternSubscribePreconditions(pattern);

    List<Topic> existingTopics = getPubsubExistingTopics();

    List<String> matchingTopics = new ArrayList<>();

    for (Topic topic: existingTopics) {
      String topicName = topic.getName().substring(TOPIC_PREFIX.length(), topic.getName().length());
      Matcher m = pattern.matcher(topicName);
      if (m.matches())
        matchingTopics.add(topicName);
    }
    subscribe(matchingTopics);

    log.debug("Subscribed to pattern: {}", pattern);
  }

  private List<Topic> getPubsubExistingTopics() {
    ListenableFuture<ListTopicsResponse> listTopicsResponseListenableFuture = publisherFutureStub
        .listTopics(ListTopicsRequest.newBuilder()
            .setProject(GOOGLE_CLOUD_PROJECT)
            .build());

    try {
      return listTopicsResponseListenableFuture.get().getTopicsList();
    } catch (InterruptedException e) {
      throw new InterruptException(e);
    } catch (ExecutionException e) {
      throw new KafkaException(e);
    }
  }

  private void checkPatternSubscribePreconditions(Pattern pattern) {
    Preconditions.checkArgument(pattern != null,
        "Topic pattern to subscribe to cannot be null");
  }

  /**
   * Unsubscribe stops all subscriptions and threads extending deadlines. If configured, deletes subscriptions it
   * was subscribed to. Resets all collections used to keep track of consumer's state.
   */
  @Override
  public void unsubscribe() {
    for(Subscriber s: topicNameToSubscriber.values()) {
      s.stopAsync().awaitTerminated();
    }

    List<Subscription> currentSubscriptions = getSubscriptionsFromSubcribers();
    deleteSubscriptionsIfAllowed(currentSubscriptions);

    topicNameToSubscriber = ImmutableMap.of();
    topicNames = ImmutableList.of();
    currentPoolIndex = 0;
  }

  private List<Subscription> getSubscriptionsFromSubcribers() {
    List<Subscription> subscriptions = new ArrayList<>(topicNameToSubscriber.size());
    for(Subscriber s: topicNameToSubscriber.values()) {
      subscriptions.add(s.getSubscription());
    }
    return subscriptions;
  }

  private void deleteSubscriptionsIfAllowed(Collection<Subscription> subscriptions) {
    if(!config.getAllowSubscriptionDeletion())
      return;

    List<ListenableFuture<Empty>> listenableFutures = new ArrayList<>();
    for (Subscription s: subscriptions) {
      ListenableFuture<Empty> emptyListenableFuture = subscriberFutureStub
          .deleteSubscription(DeleteSubscriptionRequest.newBuilder()
              .setSubscription(s.getName()).build());

      listenableFutures.add(emptyListenableFuture);
    }

    ListenableFuture<List<Empty>> listListenableFuture = Futures.allAsList(listenableFutures);
    Futures.addCallback(listListenableFuture, new DeleteSubscriptionCallback());
  }

  /**
   * First, poll checks if any calls of seekToBeginning or seekToEnd vere invoked, and if true, seeks on specific
   * topics.
   *
   * Poll works in Round Robin fashion. On each call, poll is performed on specific topic. If any messages were returned,
   * it returns ConsumerRecords containing all polled data. If no messages were returned it retries poll on next topic,
   * as long as it polls some messages or gets though all topics.
   *
   * When polling, deadline extensions are scheduled. Deadline will be extended until commit call (manual config),
   * until auto.commit.interval passes (auto config), or until max.ack.extension.period passes (maximum on deadline
   * extensions).
   *
   * If auto commit configured, every time poll is called it will check if auto.commit.interval passed since last
   * commit. If it did, it will perform a commit. If auto commit used, it is crucial to make sure all previously polled
   * messages were processed before call to another poll.
   *
   * This method will perform onConsume interceptor method, if any interceptors were configured.
   */
  public ConsumerRecords<K, V> poll(long timeout) {
    checkPollPreconditions(timeout);

    int startedAtIndex = this.currentPoolIndex;
    try {
      do {

        String topicName = topicNames.get(this.currentPoolIndex % topicNameToSubscriber.size());
        Subscriber subscriber = topicNameToSubscriber.get(topicName);

        PullResponse pullResponse = subscriber.pull(timeout);

        List<ConsumerRecord<K, V>> subscriptionRecords = mapToConsumerRecords(topicName, pullResponse);
        this.currentPoolIndex = (this.currentPoolIndex + 1) % topicNameToSubscriber.size();

        if (!pullResponse.getReceivedMessagesList().isEmpty()) {
          return getConsumerRecords(topicName, subscriptionRecords);
        }
      } while (this.currentPoolIndex != startedAtIndex);

    } catch (InterruptedException e) {
      throw new InterruptException(e);
    } catch (ExecutionException | IOException e) {
      throw new KafkaException(e);
    }

    return new ConsumerRecords<>(new HashMap<>());
  }

  private ConsumerRecords<K, V> getConsumerRecords(String topicName,
      List<ConsumerRecord<K, V>> subscriptionRecords) {
    Map<TopicPartition, List<ConsumerRecord<K, V>>> pollRecords = new HashMap<>();

    TopicPartition topicPartition = new TopicPartition(topicName, DEFAULT_PARTITION);
    pollRecords.put(topicPartition, subscriptionRecords);

    return new ConsumerRecords<>(pollRecords);
  }

  private List<ConsumerRecord<K, V>> mapToConsumerRecords(String topicName, PullResponse pulled) {
    List<ConsumerRecord<K, V>> subscriptionRecords = new ArrayList<>();

    for (ReceivedMessage receivedMessage : pulled.getReceivedMessagesList()) {
      ConsumerRecord<K, V> record = prepareKafkaRecord(receivedMessage,
          topicName);
      subscriptionRecords.add(record);
    }

    return subscriptionRecords;
  }

  private void checkPollPreconditions(long timeout) {

    Preconditions.checkArgument(timeout >= 0,
        "Timeout must not be negative");

    if (topicNameToSubscriber.isEmpty()) {
      throw new IllegalStateException("Consumer is not subscribed to any topics");
    }
  }

  private ConsumerRecord<K,V> prepareKafkaRecord(ReceivedMessage receivedMessage, String topic) {
    PubsubMessage message = receivedMessage.getMessage();

    long timestamp = message.getPublishTime().getSeconds();
    TimestampType timestampType = TimestampType.CREATE_TIME;

    //because of no offset concept in PubSub, timestamp is treated as an offset
    long offset = timestamp;

    //key of Kafka-style message is stored in PubSub attributes (null possible)
    String key = message.getAttributesOrDefault(KEY_ATTRIBUTE, null);

    byte [] deserializedKeyBytes = key != null ? Base64.decodeBase64(key.getBytes()) : null;

    //lengths of serialized value and serialized key
    int serializedValueSize = message.getData().toByteArray().length;
    int serializedKeySize = key != null ? deserializedKeyBytes.length : 0;

    V deserializedValue = config.getValueDeserializer().deserialize(topic, message.getData().toByteArray());
    K deserializedKey = key != null ? config.getKeyDeserializer().deserialize(topic, deserializedKeyBytes) : null;

    return new ConsumerRecord<>(topic, DEFAULT_PARTITION, offset, timestamp, timestampType,
        DEFAULT_CHECKSUM, serializedKeySize, serializedValueSize, deserializedKey,
        deserializedValue);
  }

  /**
   * This method performs the same way as subscribe. It get set of topics from provided collection and performs
   * subscribe on them. Partitions are ignored.
   */
  @Override
  public void assign(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Commit previously polled messages in a synchronous way (blocking)
   */
  @Override
  public void commitSync() {
    commit(true);
  }

  /**
   * For every topic in this collection, all currently not commited (but polled) messages which offset is smaller than
   * one in OffsetAndMetadata object are being commited. This behavior is different with Kafka. Synchronous.
   */
  @Override
  public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Commit previously polled messages in a asynchronous way (non-blocking)
   */
  @Override
  public void commitAsync() {
    commit(false);
  }

  /**
   * OffsetCommitCallback has no meaning in Pub/Sub. This call works exactly the same as commitAsync() (no params)
   */
  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * For every topic in this collection, all currently not commited (but polled) messages which offset is smaller than
   * one in OffsetAndMetadata object are being commited. This behavior is different with Kafka. Asynchronous.
   *
   * OffsetCommitCallback has no meaning in Pub/Sub, so this parameter is ignored.
   */
  @Override
  public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets,
      OffsetCommitCallback callback) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  private void commit(boolean sync) {
    for (Map.Entry<String, Subscriber> entry : topicNameToSubscriber.entrySet()) {
      entry.getValue().commit(sync);
    }
  }

  /**
   * Call to seek performs Pub/Sub seek on topic provided in TopicPartition object. Partition is ignored.
   *
   * This call results in marking all messages before this offset as ACKed, and all messages after this
   * timestamp as NACKed. THIS CALL CHANGES MESSAGES STATE ON THE SERVER. This is not consistent with Kafka API.
   */
  @Override
  public void seek(TopicPartition partition, long offset) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Call to seekToBeginning performs Pub/Sub seek on topic provided in TopicPartition object to the timestamp 0.
   * Partition is ignored.
   *
   * This call results in marking all messages after offset 0 as NACKed. THIS CALL CHANGES MESSAGES STATE ON THE SERVER.
   * This is not consistent with Kafka API.
   *
   * This call is evaluated in lazy way on nearest call to poll method.
   */
  @Override
  public void seekToBeginning(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Call to seekToEnd performs Pub/Sub seek on topic provided in TopicPartition object to the current timestamp.
   * Partition is ignored.
   *
   * This call results in marking all messages before current timestamp as ACKed. THIS CALL CHANGES MESSAGES STATE
   * ON THE SERVER.
   * This is not consistent with Kafka API.
   *
   * This call is evaluated in lazy way on nearest call to poll method (current timestamp will be timestamp of poll
   * start).
   */
  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * This method has absolutely no meaning in Pub/Sub. Throws exception.
   */
  @Override
  public long position(TopicPartition partition) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * This method has absolutely no meaning in Pub/Sub. Throws exception.
   */
  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Process metrics may be provided by Pub/Sub Stackdriver project. Returns empty map.
   */
  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Returns dummy partition with number 0 and dummy nodes.
   */
  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Returns list of topics available on the server, paired with dummy partition (number 0, dummy nodes).
   */
  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Blocks messages polling from all topics contained in this collection.
   */
  @Override
  public void pause(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Resumes messages polling from all topics contained in this collection.
   */
  @Override
  public void resume(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Returns set of topics for which polling is blocked, paired with dummy partition number equal to 0.
   */
  @Override
  public Set<TopicPartition> paused() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * As in our implementation timestamp is the offset, it just changes data format.
   */
  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      Map<TopicPartition, Long> timestampsToSearch) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * For every topic it returns beginning offset - in our case 0 (smallest possible timestamp).
   */
  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * For every topic it returns end offset - in our case current timestamp (biggest possible timestamp until now).
   */
  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
  Perform unsubscribe() from subscribed topics, closes deserializers, stops subscribers.
   */
  @Override
  public void close() {
    log.debug("Closing PubSub subscriber");
    unsubscribe();
    config.getKeyDeserializer().close();
    config.getValueDeserializer().close();
    log.debug("PubSub subscriber has been closed");
  }

  /**
   * This method has absolutely no meaning in Pub/Sub. Throws exception.
   */
  @Override
  public void wakeup() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  class ResponseData<T> {
    private String topicName;
    private String subscriptionFullName;
    private ListenableFuture<T> requestListenableFuture;

    ResponseData(String topicName, String subscriptionFullName,
        ListenableFuture<T> requestListenableFuture) {
      this.topicName = topicName;
      this.subscriptionFullName = subscriptionFullName;
      this.requestListenableFuture = requestListenableFuture;
    }

    String getTopicName() {
      return topicName;
    }

    String getSubscriptionFullName() {
      return subscriptionFullName;
    }

    ListenableFuture<T> getRequestListenableFuture() {
      return requestListenableFuture;
    }
  }

  class DeleteSubscriptionCallback implements FutureCallback<List<Empty>> {
    @Override
    public void onSuccess(@Nullable List<Empty> empties) {

    }

    @Override
    public void onFailure(Throwable throwable) {
      //TODO retry unsubscribe?
      log.warn("Failed to unsubscribe to topic", throwable);
    }
  }

}


