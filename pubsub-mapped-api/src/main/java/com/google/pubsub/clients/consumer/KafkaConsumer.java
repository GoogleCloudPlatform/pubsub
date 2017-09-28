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
import com.google.protobuf.Timestamp;
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
import com.google.pubsub.v1.SeekRequest;
import com.google.pubsub.v1.SeekResponse;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;
import org.joda.time.DateTime;
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
  private static final String OFFSET_ATTRIBUTE = "offset";

  private final Config<K, V> config;
  private final SubscriberFutureStub subscriberFutureStub;
  private final PublisherFutureStub publisherFutureStub;

  private ImmutableList<String> topicNames = ImmutableList.of();
  private ImmutableMap<String, Subscriber> topicNameToSubscriber = ImmutableMap.of();
  private Set<String> pausedTopics = new HashSet<>();
  private Map<String, Seek> lazySeeks = new HashMap<>();

  enum Seek {
    BEGINNING,
    END
  }

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

  @Override
  public Set<TopicPartition> assignment() {
    Set<TopicPartition> partitions = new HashSet<>();
    for(String topicName: topicNames) {
      partitions.add(new TopicPartition(topicName, DEFAULT_PARTITION));
    }
    return partitions;
  }

  @Override
  public Set<String> subscription() {
    return topicNameToSubscriber.keySet();
  }

  /**
   * Subscribe call tries to get existing subscriptions for groupId provided in configuration and topic names. If
   * consumer was configured to create subscriptions if they don't exist, on failure with NOT_FOUND status it sends
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

  @Override
  public void subscribe(Collection<String> topics) {
    subscribe(topics, NO_REBALANCE_LISTENER);
  }

  /**
  Subscribe is pulling all topics from PubSub project and subscribing to ones matching provided
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

  @Override
  public void unsubscribe() {
    for(Subscriber s: topicNameToSubscriber.values()) {
      s.stopAsync().awaitTerminated();
    }

    List<Subscription> currentSubscriptions = getSubscriptionsFromSubcribers();
    deleteSubscriptionsIfAllowed(currentSubscriptions);

    topicNameToSubscriber = ImmutableMap.of();
    topicNames = ImmutableList.of();
    pausedTopics = new HashSet<>();
    lazySeeks = new HashMap<>();
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

  public ConsumerRecords<K, V> poll(long timeout) {
    checkPollPreconditions(timeout);

    if(!lazySeeks.isEmpty()) {
      performLazySeekCalls();
    }

    int startedAtIndex = this.currentPoolIndex;
    ConsumerRecords<K, V> consumerRecords = new ConsumerRecords<>(new HashMap<>());
    try {
      do {

        String topicName = topicNames.get(this.currentPoolIndex % topicNameToSubscriber.size());
        if(!pausedTopics.contains(topicName)) {
          Subscriber subscriber = topicNameToSubscriber.get(topicName);
          PullResponse pullResponse = subscriber.pull(timeout);

          List<ConsumerRecord<K, V>> subscriptionRecords = mapToConsumerRecords(topicName, pullResponse);

          if (!pullResponse.getReceivedMessagesList().isEmpty()) {
            consumerRecords = getConsumerRecords(topicName, subscriptionRecords);
            break;
          }
        }
        this.currentPoolIndex = (this.currentPoolIndex + 1) % topicNameToSubscriber.size();
      } while (this.currentPoolIndex != startedAtIndex);

    } catch (InterruptedException e) {
      throw new InterruptException(e);
    } catch (ExecutionException | IOException e) {
      throw new KafkaException(e);
    }

    if (config.getInterceptors() != null) {
      consumerRecords = config.getInterceptors().onConsume(consumerRecords);
    }
    return consumerRecords;
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

    long timestamp = message.getPublishTime().getSeconds() * 1000 + message.getPublishTime().getNanos() / 1000;

    TimestampType timestampType = TimestampType.CREATE_TIME;

    //because of no offset concept in PubSub, timestamp is treated as an offset
    String offsetString = message.getAttributesOrDefault(OFFSET_ATTRIBUTE, "0");
    long offset = Long.parseLong(offsetString);

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

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    Set<String> topics = new HashSet<>();
    for(TopicPartition topicPartition: partitions) {
      topics.add(topicPartition.topic());
    }
    subscribe(topics);
  }

  @Override
  public void commitSync() {
    commit(true);
  }

  @Override
  public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void commitAsync() {
    commit(false);
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets,
      OffsetCommitCallback callback) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  private void commit(boolean sync) {
    for (Map.Entry<String, Subscriber> entry : topicNameToSubscriber.entrySet()) {
      entry.getValue().commit(sync);
    }

    /*We have no concept of offsets, so it is impossible to pass an argument to the interceptor that
    makes any sense. If to be resolved in the future, the call for sync call should go somewhere here.*/
    /*if (sync && interceptors != null)
      interceptors.onCommit(offsets);*/
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    seekToTimestamp(Collections.singletonList(partition.topic()), offset);
  }

  @Override
  public void seekToBeginning(Collection<TopicPartition> partitions) {
    seekToPlace(partitions, Seek.BEGINNING);
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    seekToPlace(partitions, Seek.END);
  }

  private void seekToPlace(Collection<TopicPartition> partitions, Seek seek) {
    if(partitions == null || partitions.isEmpty()) {
      for(String topic: topicNames) {
        lazySeeks.put(topic, seek);
      }
    } else {
      for(TopicPartition partition: partitions) {
        lazySeeks.put(partition.topic(), seek);
      }
    }
  }

  private void performLazySeekCalls() {
    List<String> beginningTopics = new ArrayList<>();
    List<String> endTopics = new ArrayList<>();

    for(Map.Entry<String, Seek> entry: lazySeeks.entrySet()) {
      if (Seek.BEGINNING.equals(entry.getValue())) {
        beginningTopics.add(entry.getKey());
      } else {
        endTopics.add(entry.getKey());
      }
    }

    seekToTimestamp(beginningTopics, 0);

    long now = new DateTime().getMillis();
    seekToTimestamp(endTopics, now);
  }

  private void seekToTimestamp(Collection<String> topics, long timestamp) {
    Timestamp protobufTimestamp = Timestamp.newBuilder().setSeconds(timestamp / 1000)
        .setNanos((int) ((timestamp % 1000) * 1000000)).build();

    List<ListenableFuture<SeekResponse>> seekResponses = new ArrayList<>();
    for(String topic: topics) {
      String topicSubscription = topicNameToSubscriber.get(topic).getSubscription().getName();
      ListenableFuture<SeekResponse> seek = subscriberFutureStub.seek(
          SeekRequest.newBuilder()
              .setSubscription(topicSubscription)
              .setTime(protobufTimestamp)
              .build()
      );
      seekResponses.add(seek);
    }

    ListenableFuture<List<SeekResponse>> listListenableFuture = Futures.allAsList(seekResponses);

    try {
      listListenableFuture.get();
    } catch (InterruptedException e) {
      throw new InterruptException(e);
    } catch (ExecutionException e) {
      throw new KafkaException(e);
    }
  }

  @Override
  public long position(TopicPartition partition) {
    throw new UnsupportedOperationException("This method has no mapping in PubSub");
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    throw new UnsupportedOperationException("This method has no mapping in PubSub ");
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return new HashMap<>();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    Node[] dummy = {new Node(0, "", 0)};
    return Arrays.asList(new PartitionInfo(topic, DEFAULT_PARTITION, dummy[0], dummy, dummy));
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    Map<String, List<PartitionInfo>> partitionTopicMap = new HashMap<>();

    Node[] dummy = {new Node(0, "", 0)};
    List<Topic> existingTopics = getPubsubExistingTopics();
    for(Topic topic: existingTopics) {
      partitionTopicMap.put(topic.getName(),
          Arrays.asList(new PartitionInfo(topic.getName(), DEFAULT_PARTITION, dummy[0], dummy, dummy)));
    }
    return partitionTopicMap;
  }

  @Override
  public void pause(Collection<TopicPartition> partitions) {
    for(TopicPartition partition: partitions) {
      pausedTopics.add(partition.topic());
    }
  }

  @Override
  public void resume(Collection<TopicPartition> partitions) {
    for(TopicPartition partition: partitions) {
      pausedTopics.remove(partition.topic());
    }
  }

  @Override
  public Set<TopicPartition> paused() {
    Set<TopicPartition> paused = new HashSet<>();
    for(String topic: pausedTopics) {
      paused.add(new TopicPartition(topic, DEFAULT_PARTITION));
    }
    return paused;
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      Map<TopicPartition, Long> timestampsToSearch) {
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = new HashMap<>();
    for(Map.Entry<TopicPartition, Long> entry: timestampsToSearch.entrySet()) {
      Long timestamp = entry.getValue();
      OffsetAndTimestamp offsetAndTimestamp = new OffsetAndTimestamp(timestamp, timestamp);
      offsetsForTimes.put(entry.getKey(), offsetAndTimestamp);
    }
    return offsetsForTimes;
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    return getTopicPartitionOffsetMap(partitions, 0L);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    long millis = new DateTime().getMillis();
    return getTopicPartitionOffsetMap(partitions, millis);
  }

  private Map<TopicPartition, Long> getTopicPartitionOffsetMap(Collection<TopicPartition> partitions, long offset) {
    Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
    for(TopicPartition topicPartition: partitions) {
      beginningOffsets.put(topicPartition, offset);
    }
    return beginningOffsets;
  }

  /**
  Temporary subscriptions are being created on subscribe and deleted on replacing subscriptions or
  on close.
  Closing is necessary to delete any remaining temporary subscriptions from Google Cloud Project.
   */
  @Override
  public void close() {
    log.debug("Closing PubSub subscriber");
    unsubscribe();
    config.getKeyDeserializer().close();
    config.getValueDeserializer().close();
    log.debug("PubSub subscriber has been closed");
  }

  @Override
  public void wakeup() {
    throw new UnsupportedOperationException("This method has no mapping in PubSub");
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


