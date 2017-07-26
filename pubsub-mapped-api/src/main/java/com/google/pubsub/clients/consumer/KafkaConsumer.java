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

  private static String GOOGLE_CLOUD_PROJECT;
  private static String SUBSCRIPTION_PREFIX;
  private static String TOPIC_PREFIX;
  private static int DEFAULT_PARTITION = 0;

  private ChannelUtil channelUtil;
  private SubscriberBlockingStub subscriberBlockingStub;
  private PublisherBlockingStub publisherBlockingStub;
  private Map<String, Subscription> subscriptions = new HashMap<>();

  Deserializer<K> keyDeserializer;
  Deserializer<V> valueDeserializer;

  static {
    GOOGLE_CLOUD_PROJECT = "projects/" + System.getenv("GOOGLE_CLOUD_PROJECT");
    SUBSCRIPTION_PREFIX = GOOGLE_CLOUD_PROJECT + "/subscriptions/";
    TOPIC_PREFIX = GOOGLE_CLOUD_PROJECT + "/topics/";
  }

  public KafkaConsumer(Map<String, Object> configs) {
    this.channelUtil = new ChannelUtil();
    this.subscriberBlockingStub = SubscriberGrpc.newBlockingStub(channelUtil.getChannel())
        .withCallCredentials(channelUtil.getCallCredentials());
    this.publisherBlockingStub = PublisherGrpc.newBlockingStub(channelUtil.getChannel())
        .withCallCredentials(channelUtil.getCallCredentials());
  }

  public KafkaConsumer(Map<String, Object> configs,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {

  }

  public KafkaConsumer(Properties properties) {

  }

  public KafkaConsumer(Properties properties,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {

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
    for(Subscription s: subscriptions.values()) {
      subscriberBlockingStub.deleteSubscription(DeleteSubscriptionRequest.newBuilder()
          .setSubscription(s.getName()).build());
    }
    subscriptions = new HashMap<>();
  }

  @Override
  public ConsumerRecords<K, V> poll(long timeout) {
    Map<TopicPartition, List<ConsumerRecord<K, V>>> pollRecords = new HashMap<>();

    for(Subscription s: subscriptions.values()) {

      String topic = s.getTopic().substring(TOPIC_PREFIX.length(), s.getTopic().length());
      TopicPartition partition = new TopicPartition(topic, DEFAULT_PARTITION);

      List<ConsumerRecord<K, V>> subscriptionRecords = new ArrayList<>();

      PullResponse pulled = subscriberBlockingStub.pull(PullRequest.newBuilder()
          .setSubscription(s.getName())
          .setMaxMessages(10)
          .setReturnImmediately(true).build());
      int receivedMessagesCount = pulled.getReceivedMessagesCount();

      for(int i = 0; i < receivedMessagesCount; ++i) {
        ReceivedMessage receivedMessage = pulled.getReceivedMessages(i);
        PubsubMessage message = receivedMessage.getMessage();

        long timestamp = message.getPublishTime().getSeconds();
        long offset = timestamp;
        TimestampType timestampType = TimestampType.CREATE_TIME;
        String key = message.getMessageId();

        int serializedValueSize = message.getData().toByteArray().length;
        int serializedKeySize = key.getBytes().length;


        V value = valueDeserializer.deserialize(topic, message.getData().toByteArray());
        K key2 = keyDeserializer.deserialize(topic, key.getBytes());


        ConsumerRecord<K, V> record = new ConsumerRecord<K, V>(topic, DEFAULT_PARTITION,
            offset,timestamp,timestampType, 1, serializedKeySize, serializedValueSize, key2, value);

        subscriptionRecords.add(record);
      }
      pollRecords.put(partition, subscriptionRecords);
    }

    return new ConsumerRecords<>(pollRecords);
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

  public void close() {
    unsubscribe();
    channelUtil.closeChannel();
  }

  public void close(long timeout, TimeUnit timeUnit) {
    close();
  }

  public void wakeup() {
    throw new NotImplementedException("Not yet implemented");
  }
}