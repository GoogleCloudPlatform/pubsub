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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.pubsub.common.ChannelUtil;
import com.google.pubsub.common.StubCreator;
import com.google.pubsub.kafkastubs.TopicPartition;
import com.google.pubsub.kafkastubs.common.KafkaException;
import com.google.pubsub.kafkastubs.common.Metric;
import com.google.pubsub.kafkastubs.common.MetricName;
import com.google.pubsub.kafkastubs.common.PartitionInfo;
import com.google.pubsub.kafkastubs.common.errors.InterruptException;
import com.google.pubsub.kafkastubs.common.record.TimestampType;
import com.google.pubsub.kafkastubs.common.serialization.Deserializer;
import com.google.pubsub.kafkastubs.consumer.Consumer;
import com.google.pubsub.kafkastubs.consumer.ConsumerRebalanceListener;
import com.google.pubsub.kafkastubs.consumer.ConsumerRecord;
import com.google.pubsub.kafkastubs.consumer.ConsumerRecords;
import com.google.pubsub.kafkastubs.consumer.OffsetAndMetadata;
import com.google.pubsub.kafkastubs.consumer.OffsetAndTimestamp;
import com.google.pubsub.kafkastubs.consumer.OffsetCommitCallback;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.PublisherGrpc.PublisherBlockingStub;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberBlockingStub;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaConsumer<K, V> implements Consumer<K, V> {

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

  private static final String KEY_ATTRIBUTE = "key_attribute";

  private StubCreator stubCreator;
  private ImmutableMap<String, Subscription> subscriptions = ImmutableMap.of();

  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;

  private final int maxPollRecords;

  public KafkaConsumer(Map<String, Object> configs) {
      this(new ConsumerConfig(configs), null, null);
  }

  public KafkaConsumer(Map<String, Object> configs,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
      this(new ConsumerConfig(
          ConsumerConfig.addDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
          keyDeserializer, valueDeserializer);
  }

  public KafkaConsumer(Properties properties) {
    this(new ConsumerConfig(properties), null, null);
  }

  public KafkaConsumer(Properties properties,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    this(new ConsumerConfig(
            ConsumerConfig.addDeserializerToConfig(properties, keyDeserializer, valueDeserializer)),
        keyDeserializer, valueDeserializer);
  }


  private KafkaConsumer(ConsumerConfig configs, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    this(configs, keyDeserializer, valueDeserializer, new StubCreator(ChannelUtil.getInstance()));
  }

  @SuppressWarnings("unchecked")
  KafkaConsumer(ConsumerConfig configs, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer, StubCreator stubCreator) {
    try {
      log.debug("Starting PubSub subscriber");

      Preconditions.checkNotNull(stubCreator);

      this.stubCreator = stubCreator;
      this.keyDeserializer = handleDeserializer(configs,
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer, true);
      this.valueDeserializer = handleDeserializer(configs,
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer, false);

      //this is a limit on each poll for each topic
      this.maxPollRecords = configs.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);

      log.debug("PubSub subscriber created");
    } catch (Throwable t) {
      throw new KafkaException("Failed to construct PubSub subscriber", t);
    }
  }

  private Deserializer handleDeserializer(ConsumerConfig configs,
      String configString, Deserializer providedDeserializer, boolean isKey) {
    Deserializer deserializer;
    if(providedDeserializer == null) {
      deserializer = configs.getConfiguredInstance(configString, Deserializer.class);
      deserializer.configure(configs.originals(), isKey);
    } else {
      configs.ignore(configString);
      deserializer = providedDeserializer;
    }
    return deserializer;
  }

  public Set<TopicPartition> assignment() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Set<String> subscription() {
    return subscriptions.keySet();
  }

  /*
  For every new consumer, a temporary subscription is created. After "unsubscribe" operation
  temporary subscriptions are removed. After an error while creating subscriptions the attempt
  is made to delete all temporary subscriptions made in this call to "subscribe".

  Every version of subscribe is replacing old subscriptions with new ones.

  Temporary subscription names are built as a combination of timestamp and randomly generated UUID.
   */
  public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
    checkSubscribePreconditions(topics);

    unsubscribe();
    List<ResponseData<Subscription>> futureSubscriptions = deputePubsubSubscribes(topics);
    Map<String, Subscription> subscriptionMap = getNewSubscriptionMap(futureSubscriptions);

    subscriptions = ImmutableMap.copyOf(subscriptionMap);
  }

  private List<ResponseData<Subscription>> deputePubsubSubscribes(Collection<String> topics) {
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

    List<ResponseData<Subscription>> responseDatas = new ArrayList<>();
    Set<String> usedNames = new HashSet<>();

    for(String topic: topics) {
      if(!usedNames.contains(topic)) {
        String subscriptionString = SUBSCRIPTION_PREFIX + topic + "_" + timestamp.getTime()
            + "_" + UUID.randomUUID();
        ListenableFuture<Subscription> deputedsubscription =
            deputeSinglePubsubSubscription(subscriptionString, topic);

        responseDatas.add(new ResponseData<>(topic, subscriptionString, deputedsubscription));
        usedNames.add(topic);
      }
    }
    return responseDatas;
  }

  private ListenableFuture<Subscription> deputeSinglePubsubSubscription(String subscriptionString,
      String topicName) {
    SubscriberFutureStub subscriberFutureStub = stubCreator.getSubscriberFutureStub();
    return subscriberFutureStub
        .createSubscription(Subscription.newBuilder()
            .setName(subscriptionString)
            .setTopic(TOPIC_PREFIX + topicName)
            .build());
  }

  private Map<String, Subscription> getNewSubscriptionMap(
      List<ResponseData<Subscription>> responseDatas) {
    Map<String, Subscription> subscriptionMap = new HashMap<>();

    for(ResponseData<Subscription> responseData: responseDatas) {
      boolean success = false;
      try {
        Subscription s = responseData.getRequestListenableFuture().get();
        subscriptionMap.put(responseData.getTopicName(), s);

        success = true;

      } catch (InterruptedException e) {
        //if an error is thrown, attempt to delete subscriptions created in this loop
        throw new InterruptException(e);
      } catch (ExecutionException e) {
        throw new KafkaException(e);
      } finally {
        if(!success)
          unsubscribe(subscriptionMap.values());
      }
    }
    return subscriptionMap;
  }

  private void checkSubscribePreconditions(Collection<String> topics) {
    Preconditions.checkArgument(topics != null,
        "Topic collection to subscribe to cannot be null");

    for(String topic: topics) {
      Preconditions.checkArgument(topic != null && !topic.trim().isEmpty(),
          "Topic collection to subscribe to cannot contain null or empty topic");
    }
  }

  @Override
  public void subscribe(Collection<String> topics) {
    subscribe(topics, NO_REBALANCE_LISTENER);
  }

  /*
  Subscribe is pulling all topics from PubSub project and subscribing to ones matching provided
  pattern.
   */
  @Override
  public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
    checkPatternSubscribePreconditions(pattern);

    List<Topic> existingTopics = getPubsubExistionTopics();

    List<String> matchingTopics = new ArrayList<>();

    for(Topic topic: existingTopics) {
      String topicName = topic.getName().substring(TOPIC_PREFIX.length(), topic.getName().length());
      Matcher m = pattern.matcher(topicName);
      if(m.matches())
        matchingTopics.add(topicName);
    }
    subscribe(matchingTopics);

    log.debug("Subscribed to pattern: {}", pattern);
  }

  private List<Topic> getPubsubExistionTopics() {
    PublisherBlockingStub publisherBlockingStub = stubCreator.getPublisherBlockingStub();

    ListTopicsResponse listTopicsResponse = publisherBlockingStub
        .listTopics(ListTopicsRequest.newBuilder()
            .setProject(GOOGLE_CLOUD_PROJECT)
            .build());

    return listTopicsResponse.getTopicsList();
  }

  private void checkPatternSubscribePreconditions(Pattern pattern) {
    Preconditions.checkArgument(pattern != null,
        "Topic pattern to subscribe to cannot be null");
  }

  @Override
  public void unsubscribe() {
    unsubscribe(subscriptions.values());
    subscriptions = ImmutableMap.of();
  }

  private void unsubscribe(Collection<Subscription> subscriptions) {
    for(Subscription s: subscriptions) {
      SubscriberFutureStub subscriberFutureStub = stubCreator.getSubscriberFutureStub();
      ListenableFuture<Empty> emptyListenableFuture = subscriberFutureStub
          .deleteSubscription(DeleteSubscriptionRequest.newBuilder()
              .setSubscription(s.getName()).build());

      Futures.addCallback(emptyListenableFuture,
          new FutureCallback<Empty>() {
            @Override
            public void onSuccess(@Nullable Empty empties) {
              log.debug("Unsubscribed to " + s.getTopic());
            }

            @Override
            public void onFailure(Throwable throwable) {
              //TODO retry unsubscribe?
              log.warn("Failed to unsubscribe to " + s.getTopic(), throwable);
            }
          });
    }
  }

  @Override
  public ConsumerRecords<K, V> poll(long timeout) {
    checkPollPreconditions(timeout);

    Map<TopicPartition, List<ConsumerRecord<K, V>>> pollRecords = new HashMap<>();
    List<ResponseData<PullResponse>> responsesFutures = deputePubsubPulls(timeout);

    List<AcknowledgeRequest> acknowledgeRequests = new ArrayList<>();

    for(ResponseData<PullResponse> pollData : responsesFutures) {
      try {
        TopicPartition topicPartition = new TopicPartition(pollData.getTopicName(),
            DEFAULT_PARTITION);

        PullResponse pulled = pollData.getRequestListenableFuture().get();

        List<ConsumerRecord<K, V>> subscriptionRecords = mapToConsumerRecords(pollData,
            pulled);

        if(!pulled.getReceivedMessagesList().isEmpty()) {
          AcknowledgeRequest acknowledgeRequest = getAcknowledgeRequest(
              pollData.getSubscriptionFullName(), pulled);
          acknowledgeRequests.add(acknowledgeRequest);
        }

        pollRecords.put(topicPartition, subscriptionRecords);
      } catch (InterruptedException e) {
        throw new InterruptException(e);
      } catch (ExecutionException e) {
        throw new KafkaException(e);
      }
    }

    //if all messages were pulled correctly, send ACK
    acknowledgeMessages(acknowledgeRequests);

    return new ConsumerRecords<>(pollRecords);
  }

  private AcknowledgeRequest getAcknowledgeRequest(String subscription,
      PullResponse pulled) {
    List<String> ackIds = new ArrayList<>();
    for(ReceivedMessage receivedMessage : pulled.getReceivedMessagesList()) {
      ackIds.add(receivedMessage.getAckId());
    }

    return AcknowledgeRequest.newBuilder()
        .addAllAckIds(ackIds)
        .setSubscription(subscription).build();
  }

  private List<ResponseData<PullResponse>> deputePubsubPulls(long timeout) {
    List<ResponseData<PullResponse>> responsesFutures = new ArrayList<>(subscriptions.size());

    for(Subscription s : subscriptions.values()) {
      String topicName = s.getTopic().substring(TOPIC_PREFIX.length(), s.getTopic().length());
      ListenableFuture<PullResponse> deputedPull = deputeSinglePubsubPull(s, timeout);
      responsesFutures.add(new ResponseData<>(topicName, s.getName(), deputedPull));
    }
    return responsesFutures;
  }

  private ListenableFuture<PullResponse> deputeSinglePubsubPull(Subscription s, long timeout) {
    SubscriberFutureStub futureStub = stubCreator.getSubscriberFutureStub(timeout);
    return futureStub.pull(PullRequest.newBuilder()
        .setSubscription(s.getName())
        .setMaxMessages(this.maxPollRecords)
        .setReturnImmediately(true).build());
  }

  private List<ConsumerRecord<K, V>> mapToConsumerRecords(ResponseData<PullResponse> pollData,
      PullResponse pulled) {

    List<ConsumerRecord<K, V>> subscriptionRecords = new ArrayList<>();

    for(ReceivedMessage receivedMessage : pulled.getReceivedMessagesList()) {
      ConsumerRecord<K, V> record = prepareKafkaRecord(receivedMessage,
          pollData.getTopicName());
      subscriptionRecords.add(record);
    }

    return subscriptionRecords;
  }

  private void acknowledgeMessages(List<AcknowledgeRequest> acknowledgeRequests) {
    for(AcknowledgeRequest acknowledgeRequest : acknowledgeRequests) {
      SubscriberBlockingStub blockingStub = stubCreator.getSubscriberBlockingStub();
      blockingStub.acknowledge(acknowledgeRequest);
    }
  }

  private void checkPollPreconditions(long timeout) {

    Preconditions.checkArgument(timeout >= 0,
        "Timeout must not be negative");

    if(subscriptions.isEmpty()) {
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

    //lengths of serialized value and serialized key
    int serializedValueSize = message.getData().toByteArray().length;
    int serializedKeySize = key != null ? key.getBytes().length : 0;

    V deserializedValue = valueDeserializer.deserialize(topic, message.getData().toByteArray());
    K deserializedKey = key != null ? keyDeserializer.deserialize(topic, key.getBytes()) : null;

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

  public void seekToBeginning(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void seekToEnd(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public long position(TopicPartition partition) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public OffsetAndMetadata committed(TopicPartition partition) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Map<MetricName, ? extends Metric> metrics() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public List<PartitionInfo> partitionsFor(String topic) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Map<String, List<PartitionInfo>> listTopics() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void pause(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void resume(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Set<TopicPartition> paused() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      Map<TopicPartition, Long> timestampsToSearch) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /*
  Temporary subscriptions are being created on subscribe and deleted on replacing subscriptions or
  on close.
  Closing is necessary to delete any remaining temporary subscriptions from Google Cloud Project.
   */
  public void close() {
    log.debug("Closing PubSub subscriber");
    unsubscribe();
    keyDeserializer.close();
    valueDeserializer.close();
    log.debug("PubSub subscriber has been closed");
  }

  public void close(long timeout, TimeUnit timeUnit) {
    close();
  }

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
}


