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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.pubsub.common.ChannelUtil;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherFutureStub;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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

  private final SubscriberFutureStub subscriberFutureStub;
  private final PublisherFutureStub publisherFutureStub;
  private final Boolean allowSubscriptionCreation;
  private final Boolean allowSubscriptionDeletion;

  private final String groupId;
  private final int maxPollRecords;

  private ImmutableMap<String, Subscription> topicNameToSubscription = ImmutableMap.of();
  private ImmutableList<String> topicNames = ImmutableList.of();

  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;

  private int currentPoolIndex;

  public KafkaConsumer(Map<String, Object> configs) {
      this(new ConsumerConfig(configs), null, null);
  }

  public KafkaConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
      this(new ConsumerConfig(
          ConsumerConfig.addDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
          keyDeserializer, valueDeserializer);
  }

  public KafkaConsumer(Properties properties) {
    this(new ConsumerConfig(properties), null, null);
  }

  public KafkaConsumer(Properties properties, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    this(new ConsumerConfig(
            ConsumerConfig.addDeserializerToConfig(properties, keyDeserializer, valueDeserializer)),
        keyDeserializer, valueDeserializer);
  }


  private KafkaConsumer(ConsumerConfig configs, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    this(configs,
        keyDeserializer,
        valueDeserializer,
        ChannelUtil.getInstance().getChannel(),
        ChannelUtil.getInstance().getCallCredentials());
  }

  @SuppressWarnings("unchecked")
  KafkaConsumer(ConsumerConfig configs, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer, Channel channel, CallCredentials callCredentials) {
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

      this.keyDeserializer = handleDeserializer(configs,
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer, true);
      this.valueDeserializer = handleDeserializer(configs,
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer, false);
      this.allowSubscriptionCreation = configs.getBoolean(ConsumerConfig.SUBSCRIPTION_ALLOW_CREATE_CONFIG);
      this.allowSubscriptionDeletion = configs.getBoolean(ConsumerConfig.SUBSCRIPTION_ALLOW_DELETE_CONFIG);
      this.groupId = configs.getString(ConsumerConfig.GROUP_ID_CONFIG);
      //this is a limit on each poll for each topic
      this.maxPollRecords = configs.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);

      Preconditions.checkNotNull(this.allowSubscriptionCreation);
      Preconditions.checkNotNull(this.allowSubscriptionDeletion);
      Preconditions.checkNotNull(this.groupId);
      Preconditions.checkArgument(!this.groupId.isEmpty());

      log.debug("PubSub subscriber created");
    } catch (Throwable t) {
      throw new KafkaException("Failed to construct PubSub subscriber", t);
    }
  }

  private Deserializer handleDeserializer(ConsumerConfig configs, String configString,
      Deserializer providedDeserializer, boolean isKey) {
    Deserializer deserializer;
    if (providedDeserializer == null) {
      deserializer = configs.getConfiguredInstance(configString, Deserializer.class);
      deserializer.configure(configs.originals(), isKey);
    } else {
      configs.ignore(configString);
      deserializer = providedDeserializer;
    }
    return deserializer;
  }

  @Override
  public Set<TopicPartition> assignment() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public Set<String> subscription() {
    return topicNameToSubscription.keySet();
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

    topicNameToSubscription = ImmutableMap.copyOf(subscriptionMap);
    topicNames = ImmutableList.copyOf(subscriptionMap.keySet());
    currentPoolIndex = 0;

    log.debug("Subscribed to topic(s): {}", Utils.join(topics, ", "));
  }

  private List<ResponseData<Subscription>> deputePubsubSubscribesGet(Collection<String> topics) {
    List<ResponseData<Subscription>> responseDatas = new ArrayList<>();
    Set<String> usedNames = new HashSet<>();

    for (String topic: topics) {
      if (!usedNames.contains(topic)) {
        String subscriptionString = SUBSCRIPTION_PREFIX + topic + "_" + this.groupId;
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
    return this.allowSubscriptionCreation && e.getCause() instanceof StatusRuntimeException
        && ((StatusRuntimeException)e.getCause()).getStatus().getCode().equals(Code.NOT_FOUND);
  }

  private List<ResponseData<Subscription>> deputePubsubSubscribes(Collection<String> topics) {
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    List<ResponseData<Subscription>> responseDatas = new ArrayList<>();
    Set<String> usedNames = new HashSet<>();

    for (String topic: topics) {
      if (!usedNames.contains(topic)) {
        String subscriptionString = SUBSCRIPTION_PREFIX + topic + "_" + timestamp.getTime()
            + "_" + UUID.randomUUID();
        ListenableFuture<Subscription> deputedSubscription =
            deputeSinglePubsubSubscription(subscriptionString, topic);

        responseDatas.add(new ResponseData<>(topic, subscriptionString, deputedSubscription));
        usedNames.add(topic);
      }
    }
    return responseDatas;
  }

  private ListenableFuture<Subscription> deputeSinglePubsubSubscription(String subscriptionString,
      String topicName) {
    return subscriberFutureStub
        .createSubscription(Subscription.newBuilder()
            .setName(subscriptionString)
            .setTopic(TOPIC_PREFIX + topicName)
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
    deleteSubscriptionsIfAllowed(topicNameToSubscription.values());
    topicNameToSubscription = ImmutableMap.of();
    topicNames = ImmutableList.of();
    currentPoolIndex = 0;
  }

  private void deleteSubscriptionsIfAllowed(Collection<Subscription> subscriptions) {
    if(!allowSubscriptionDeletion)
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

    int startedAtIndex = this.currentPoolIndex;
    try {
      do {
        ResponseData<PullResponse> pollData = getPullResponseResponseData(timeout);
        PullResponse pullResponse = pollData.getRequestListenableFuture().get();

        List<ConsumerRecord<K, V>> subscriptionRecords = mapToConsumerRecords(pollData, pullResponse);

        this.currentPoolIndex = (this.currentPoolIndex + 1) % topicNameToSubscription.size();

        if (!subscriptionRecords.isEmpty()) {
          AcknowledgeRequest acknowledgeRequest = getAcknowledgeRequest(pollData.getSubscriptionFullName(),
              pullResponse);
          //TODO depute acknowledge message with timeout rather than ack immediately
          acknowledgeMessage(acknowledgeRequest);

          return getConsumerRecords(pollData, subscriptionRecords);
        }
      } while (this.currentPoolIndex != startedAtIndex);

    } catch (InterruptedException e) {
      throw new InterruptException(e);
    } catch (ExecutionException e) {
      throw new KafkaException(e);
    }

    return new ConsumerRecords<>(new HashMap<>());
  }

  private ConsumerRecords<K, V> getConsumerRecords(ResponseData<PullResponse> pollData,
      List<ConsumerRecord<K, V>> subscriptionRecords) {
    Map<TopicPartition, List<ConsumerRecord<K, V>>> pollRecords = new HashMap<>();

    TopicPartition topicPartition = new TopicPartition(pollData.getTopicName(), DEFAULT_PARTITION);
    pollRecords.put(topicPartition, subscriptionRecords);

    return new ConsumerRecords<>(pollRecords);
  }

  private ResponseData<PullResponse> getPullResponseResponseData(long timeout) {
    String topicName = topicNames.get(this.currentPoolIndex % topicNameToSubscription.size());

    Subscription subscription = topicNameToSubscription.get(topicName);
    ListenableFuture<PullResponse> deputedPull = deputeSinglePubsubPull(subscription, timeout);
    return new ResponseData<>(topicName, subscription.getName(), deputedPull);
  }

  private AcknowledgeRequest getAcknowledgeRequest(String subscription, PullResponse pulled) {
    List<String> ackIds = new ArrayList<>();
    for (ReceivedMessage receivedMessage : pulled.getReceivedMessagesList()) {
      ackIds.add(receivedMessage.getAckId());
    }

    return AcknowledgeRequest.newBuilder()
        .addAllAckIds(ackIds)
        .setSubscription(subscription).build();
  }

  private ListenableFuture<PullResponse> deputeSinglePubsubPull(Subscription s, long timeout) {
    SubscriberFutureStub deadlineFutureStub =
        subscriberFutureStub.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS);

    return deadlineFutureStub.pull(PullRequest.newBuilder()
        .setSubscription(s.getName())
        .setMaxMessages(this.maxPollRecords)
        .setReturnImmediately(true).build());
  }

  private List<ConsumerRecord<K, V>> mapToConsumerRecords(ResponseData<PullResponse> pollData,
      PullResponse pulled) {
    List<ConsumerRecord<K, V>> subscriptionRecords = new ArrayList<>();

    for (ReceivedMessage receivedMessage : pulled.getReceivedMessagesList()) {
      ConsumerRecord<K, V> record = prepareKafkaRecord(receivedMessage,
          pollData.getTopicName());
      subscriptionRecords.add(record);
    }

    return subscriptionRecords;
  }

  private void acknowledgeMessage(AcknowledgeRequest acknowledgeRequest)
      throws ExecutionException, InterruptedException {
    ListenableFuture<Empty> acknowledgeFuture = subscriberFutureStub.acknowledge(acknowledgeRequest);
    acknowledgeFuture.get();
  }

  private void checkPollPreconditions(long timeout) {

    Preconditions.checkArgument(timeout >= 0,
        "Timeout must not be negative");

    if (topicNameToSubscription.isEmpty()) {
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

    byte [] deserializedKeyBytes = Base64.decodeBase64(key.getBytes());

    //lengths of serialized value and serialized key
    int serializedValueSize = message.getData().toByteArray().length;
    int serializedKeySize = key != null ? deserializedKeyBytes.length : 0;

    V deserializedValue = valueDeserializer.deserialize(topic, message.getData().toByteArray());
    K deserializedKey = key != null ? keyDeserializer.deserialize(topic, deserializedKeyBytes) : null;

    return new ConsumerRecord<>(topic, DEFAULT_PARTITION, offset, timestamp, timestampType,
        DEFAULT_CHECKSUM, serializedKeySize, serializedValueSize, deserializedKey,
        deserializedValue);
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void commitSync() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void commitAsync() {
    throw new UnsupportedOperationException("Not yet implemented");
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

  @Override
  public void seek(TopicPartition partition, long offset) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void seekToBeginning(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public long position(TopicPartition partition) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void pause(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void resume(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public Set<TopicPartition> paused() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      Map<TopicPartition, Long> timestampsToSearch) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
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
    keyDeserializer.close();
    valueDeserializer.close();
    log.debug("PubSub subscriber has been closed");
  }

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


