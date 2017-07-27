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

package com.google.pubsub.clients.consumer;

import com.google.common.base.Optional;
import com.google.pubsub.clients.producer.PubsubProducerConfig;
import com.google.pubsub.common.ChannelUtil;
import com.google.pubsub.kafkastubs.TopicPartition;
import com.google.pubsub.kafkastubs.common.Metric;
import com.google.pubsub.kafkastubs.common.MetricName;
import com.google.pubsub.kafkastubs.common.PartitionInfo;
import com.google.pubsub.kafkastubs.common.record.TimestampType;
import com.google.pubsub.kafkastubs.common.serializatiom.Deserializer;
import com.google.pubsub.kafkastubs.consumer.*;
import com.google.pubsub.kafkastubs.consumer.ConsumerRecords;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherBlockingStub;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberBlockingStub;
import com.google.pubsub.v1.Subscription;

import com.google.pubsub.v1.Topic;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import org.apache.commons.lang3.NotImplementedException;


import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import com.google.pubsub.v1.SubscriberGrpc;

public class KafkaConsumer<K, V> implements Consumer<K, V> {

  private static final String GOOGLE_CLOUD_PROJECT;
  private static final String SUBSCRIPTION_PREFIX;
  private static final String TOPIC_PREFIX;

  //because of no partition concept, the partition number is constant
  private static final int DEFAULT_PARTITION = 0;

  //because checksum is not needed, it is defined as a constant
  private static final int DEFAULT_CHECKSUM = 1;

  private static final String KEY_ATTRIBUTE = "key_attribute";

  private ChannelUtil channelUtil;
  private Map<String, Subscription> subscriptions = new HashMap<>();

  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;

  private final int maxPollRecords;

  static {
    GOOGLE_CLOUD_PROJECT = "projects/" + System.getenv("GOOGLE_CLOUD_PROJECT");
    SUBSCRIPTION_PREFIX = GOOGLE_CLOUD_PROJECT + "/subscriptions/";
    TOPIC_PREFIX = GOOGLE_CLOUD_PROJECT + "/topics/";
  }

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
    this.channelUtil = ChannelUtil.getInstance();

    this.keyDeserializer = handleDeserializer(configs,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer, true);
    this.valueDeserializer = handleDeserializer(configs,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer, false);

    this.maxPollRecords = configs.getInt(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
  }

  private SubscriberBlockingStub getSubscriberBlockingStub() {
    return SubscriberGrpc.newBlockingStub(channelUtil.getChannel())
        .withCallCredentials(channelUtil.getCallCredentials());
  }

  private PublisherBlockingStub getPublisherBlockingStub() {
    return PublisherGrpc.newBlockingStub(channelUtil.getChannel())
        .withCallCredentials(channelUtil.getCallCredentials());
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
    throw new NotImplementedException("Not yet implemented");
  }

  public Set<String> subscription() {
    return subscriptions.keySet();
  }

  public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
    unsubscribe();
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    SubscriberBlockingStub subscriberBlockingStub = getSubscriberBlockingStub();
    
    for(String topic: topics) {
      if(!subscriptions.containsKey(topic)) {
        Subscription subscription = subscriberBlockingStub.createSubscription(Subscription.newBuilder()
            .setName(SUBSCRIPTION_PREFIX + topic + "_" + timestamp
                    .getTime())
            .setTopic(TOPIC_PREFIX + topic)
            .build());
        subscriptions.put(topic, subscription);
      }
    }
  }

  @Override
  public void subscribe(Collection<String> topics) {
    subscribe(topics, null);
  }

  @Override
  public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
    PublisherBlockingStub publisherBlockingStub = getPublisherBlockingStub();

    ListTopicsResponse listTopicsResponse = publisherBlockingStub
        .listTopics(ListTopicsRequest.newBuilder()
            .setProject(GOOGLE_CLOUD_PROJECT)
            .build());

    int topicsCount = listTopicsResponse.getTopicsCount();
    List<String> matchingTopics = new ArrayList<>();

    for(int i = 0; i < topicsCount; ++i) {
      Topic topic = listTopicsResponse.getTopics(i);
      String topicName = topic.getName().substring(TOPIC_PREFIX.length(), topic.getName().length());
      Matcher m = pattern.matcher(topicName);
      if(m.matches())
        matchingTopics.add(topicName);
    }
    subscribe(matchingTopics);
  }

  @Override
  public void unsubscribe() {
    SubscriberBlockingStub subscriberBlockingStub = getSubscriberBlockingStub();
    
    for(Subscription s: subscriptions.values()) {
      subscriberBlockingStub.deleteSubscription(DeleteSubscriptionRequest.newBuilder()
          .setSubscription(s.getName()).build());
    }
    subscriptions = new HashMap<>();
  }

  @Override
  public ConsumerRecords<K, V> poll(long timeout) {

    //TODO asynch polling from separate subscriptions
    Map<TopicPartition, List<ConsumerRecord<K, V>>> pollRecords = new HashMap<>();
    for(Subscription s: subscriptions.values()) {
      List<ConsumerRecord<K, V>> subscriptionRecords = new ArrayList<>();

      String topic = s.getTopic().substring(TOPIC_PREFIX.length(), s.getTopic().length());
      TopicPartition topicPartition = new TopicPartition(topic, DEFAULT_PARTITION);

      PullResponse pulled = pubsubPull(s);
      int receivedMessagesCount = pulled.getReceivedMessagesCount();

      for(int i = 0; i < receivedMessagesCount; ++i) {
        ReceivedMessage receivedMessage = pulled.getReceivedMessages(i);
        ConsumerRecord<K, V> record = prepareKafkaRecord(receivedMessage, topic);
        subscriptionRecords.add(record);
      }

      pollRecords.put(topicPartition, subscriptionRecords);
    }

    return new ConsumerRecords<>(pollRecords);
  }

  private PullResponse pubsubPull(Subscription s) {
    SubscriberBlockingStub subscriberBlockingStub = getSubscriberBlockingStub();
    return subscriberBlockingStub.pull(PullRequest.newBuilder()
        .setSubscription(s.getName())
        .setMaxMessages(this.maxPollRecords)
        .setReturnImmediately(true).build());
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
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void commitSync() {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void commitAsync() {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets,
      OffsetCommitCallback callback) {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    throw new NotImplementedException("Not yet implemented");
  }

  public void seekToBeginning(Collection<TopicPartition> partitions) {
    throw new NotImplementedException("Not yet implemented");
  }

  public void seekToEnd(Collection<TopicPartition> partitions) {
    throw new NotImplementedException("Not yet implemented");
  }

  public long position(TopicPartition partition) {
    throw new NotImplementedException("Not yet implemented");
  }

  public OffsetAndMetadata committed(TopicPartition partition) {
    throw new NotImplementedException("Not yet implemented");
  }

  public Map<MetricName, ? extends Metric> metrics() {
    throw new NotImplementedException("Not yet implemented");
  }

  public List<PartitionInfo> partitionsFor(String topic) {
    throw new NotImplementedException("Not yet implemented");
  }

  public Map<String, List<PartitionInfo>> listTopics() {
    throw new NotImplementedException("Not yet implemented");
  }

  public void pause(Collection<TopicPartition> partitions) {
    throw new NotImplementedException("Not yet implemented");
  }

  public void resume(Collection<TopicPartition> partitions) {
    throw new NotImplementedException("Not yet implemented");
  }

  public Set<TopicPartition> paused() {
    throw new NotImplementedException("Not yet implemented");
  }

  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
    throw new NotImplementedException("Not yet implemented");
  }

  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    throw new NotImplementedException("Not yet implemented");
  }

  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    throw new NotImplementedException("Not yet implemented");
  }

  /*
  Temporary subscriptions are being created on subscribe and deleted on replacing subscriptions or
  on close.
  Closing is necessary to delete any remaining temporary subscriptions from Google Cloud Project.
   */
  public void close() {
    unsubscribe();
  }

  public void close(long timeout, TimeUnit timeUnit) {
    close();
  }

  public void wakeup() {
    throw new NotImplementedException("Not yet implemented");
  }
}