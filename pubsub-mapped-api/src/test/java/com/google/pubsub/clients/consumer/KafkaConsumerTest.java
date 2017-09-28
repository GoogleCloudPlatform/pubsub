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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.client.util.Base64;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.ListTopicsResponse.Builder;
import com.google.pubsub.v1.PublisherGrpc.PublisherImplBase;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SeekRequest;
import com.google.pubsub.v1.SeekResponse;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberImplBase;
import com.google.pubsub.v1.Subscription;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;


@RunWith(JUnit4.class)
@SuppressStaticInitializationFor("com.google.pubsub.common.ChannelUtil")
public class KafkaConsumerTest {

  @Rule
  public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();

  @Test
  public void subscribeOnlyGet() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    grpcServerRule.getServiceRegistry().addService(new SubscriberGetImpl());

    Set<String> topics = new HashSet<>(Arrays.asList("topic1", "topic2"));
    consumer.subscribe(Arrays.asList("topic2", "topic1"));

    Set<String> subscribed = consumer.subscription();
    assertEquals(topics, subscribed);
    consumer.close();
  }

  @Test
  public void subscribeNoExistingSubscriptionsAllowCreation() {
    KafkaConsumer<Integer, String> consumer = getConsumer(true);
    grpcServerRule.getServiceRegistry().addService(new SubscriberNoExistingSubscriptionsImpl());

    Set<String> topics = new HashSet<>(Arrays.asList("topic1", "topic2"));
    consumer.subscribe(Arrays.asList("topic2", "topic1"));

    Set<String> subscribed = consumer.subscription();
    assertEquals(topics, subscribed);
    consumer.close();
  }

  @Test
  public void subscribeNoExistingSubscriptionsDontAllowCreation() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    grpcServerRule.getServiceRegistry().addService(new SubscriberNoExistingSubscriptionsImpl());

    try {
      consumer.subscribe(Arrays.asList("topic2", "topic1"));
      fail();
    } catch (KafkaException e) {
      //expected
    }
    consumer.close();
  }

  @Test
  public void unsubscribe() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    grpcServerRule.getServiceRegistry().addService(new SubscriberGetImpl());

    consumer.subscribe(Arrays.asList("topic2", "topic1"));
    consumer.unsubscribe();

    assertTrue(consumer.subscription().isEmpty());
    consumer.close();
  }

  @Test
  public void resubscribe() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    grpcServerRule.getServiceRegistry().addService(new SubscriberGetImpl());

    Set<String> topics = new HashSet<>(Arrays.asList("topic4", "topic3"));

    consumer.subscribe(Arrays.asList("topic2", "topic1"));
    consumer.subscribe(Arrays.asList("topic3", "topic4"));

    Set<String> subscribed = consumer.subscription();
    assertEquals(topics, subscribed);
    consumer.close();
  }

  @Test
  public void subscribeWithRecurringTopics() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    grpcServerRule.getServiceRegistry().addService(new SubscriberGetImpl());

    Set<String> topics = new HashSet<>(Arrays.asList("topic", "topic1"));

    consumer.subscribe(Arrays.asList("topic", "topic1", "topic"));

    Set<String> subscribed = consumer.subscription();
    assertEquals(topics, subscribed);
    consumer.close();
  }

  @Test
  public void patternSubscription() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    grpcServerRule.getServiceRegistry().addService(new SubscriberGetImpl());
    grpcServerRule.getServiceRegistry().addService(new PublisherImpl());

    Pattern pattern = Pattern.compile("[a-z]*\\d{3}cat");

    consumer.subscribe(pattern, null);
    Set<String> subscribed = consumer.subscription();
    Set<String> expectedTopics = new HashSet<>(Arrays.asList("thisis123cat", "funnycats000cat"));
    assertEquals(expectedTopics, subscribed);
    consumer.close();
  }

  @Test
  public void emptyPatternFails() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    try {
      Pattern pattern = null;
      consumer.subscribe(pattern, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Topic pattern to subscribe to cannot be null", e.getMessage());
    }
    consumer.close();
  }

  @Test
  public void nullTopicListFails() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    try {
      List<String> topics = null;
      consumer.subscribe(topics);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Topic collection to subscribe to cannot be null", e.getMessage());
    }
    consumer.close();
  }

  @Test
  public void emptyTopicFails() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    try {
      List<String> topics = new ArrayList<>(Collections.singletonList("    "));
      consumer.subscribe(topics);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Topic collection to subscribe to cannot contain null or empty topic", e.getMessage());
    }
    consumer.close();
  }

  @Test
  public void noSubscriptionsPollFails() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    try {
      consumer.poll(100);
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Consumer is not subscribed to any topics", e.getMessage());
    }
    consumer.close();
  }

  @Test
  public void negativePollTimeoutFails() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    try {
      consumer.poll(-200);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Timeout must not be negative", e.getMessage());
    }
    consumer.close();
  }

  @Test
  public void deserializeProperly() throws ExecutionException, InterruptedException {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    grpcServerRule.getServiceRegistry().addService(new SubscriberGetImpl());

    String topic = "topic";
    Integer key = 125;
    String value = "value";
    consumer.subscribe(Collections.singletonList(topic));

    ConsumerRecords<Integer, String> consumerRecord = consumer.poll(100);

    Iterable<ConsumerRecord<Integer, String>> recordsForTopic =
        consumerRecord.records("topic");

    ConsumerRecord<Integer, String> next = recordsForTopic.iterator().next();

    assertEquals(consumerRecord.count(), 1);
    assertEquals(value, next.value());
    assertEquals(key, next.key());
    consumer.close();
  }

  @Test
  public void pollExecutionException() throws ExecutionException, InterruptedException {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    grpcServerRule.getServiceRegistry().addService(new ErrorSubscriberImpl());

    String topic = "topic";
    consumer.subscribe(Collections.singletonList(topic));
    try {
      consumer.poll(100);
      fail();
    } catch (KafkaException e) {
      //expected
    }
  }

  @Test
  public void assignment() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    grpcServerRule.getServiceRegistry().addService(new SubscriberGetImpl());

    Set<String> topics = new HashSet<>(Arrays.asList("topic1", "topic2"));
    consumer.assign(Arrays.asList(new TopicPartition("topic2", 0), new TopicPartition("topic1", 0)));

    Set<TopicPartition> assignment = consumer.assignment();
    Set<String> partitionTopics = assignment.stream().map(TopicPartition::topic).collect(Collectors.toSet());
    List<Integer> partitionOffsets  = assignment.stream().map(TopicPartition::partition).collect(Collectors.toList());

    assertEquals(topics, partitionTopics);
    assertEquals(Arrays.asList(0, 0), partitionOffsets);
    consumer.close();
  }

  @Test
  public void seekTimestamp() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    grpcServerRule.getServiceRegistry().addService(new SubscriberGetImpl());

    String topic = "topic";
    consumer.subscribe(Collections.singletonList(topic));

    long offset = 12345678000L;
    consumer.seek(new TopicPartition(topic, 0), offset);
    ConsumerRecords<Integer, String> poll = consumer.poll(1000);

    for(ConsumerRecord record : poll.records(topic)) {
      assertEquals(record.offset(), offset);
    }
  }

  @Test
  public void seekToBeginning() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    grpcServerRule.getServiceRegistry().addService(new SubscriberGetImpl());

    String topic = "topic";
    consumer.subscribe(Collections.singletonList(topic));
    consumer.seekToBeginning(Collections.singletonList(new TopicPartition(topic, 0)));

    ConsumerRecords<Integer, String> poll = consumer.poll(1000);

    for(ConsumerRecord record : poll.records(topic)) {
      assertEquals(record.offset(), 0);
    }
  }

  @Test
  public void seekToBeginningEmptyList() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    grpcServerRule.getServiceRegistry().addService(new SubscriberGetImpl());

    String topic = "topic";
    consumer.subscribe(Collections.singletonList(topic));
    consumer.seekToBeginning(new ArrayList<>());

    ConsumerRecords<Integer, String> poll = consumer.poll(1000);

    for(ConsumerRecord record : poll.records(topic)) {
      assertEquals(record.offset(), 0);
    }
  }

  @Test
  public void checkBeginningOffsets() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    List<TopicPartition> partitions = Arrays.asList(new TopicPartition("topic1", 0),
                                                    new TopicPartition("topic2", 0));
    Map<TopicPartition, Long> offsetsMap = consumer.beginningOffsets(partitions);
    for(Long offset: offsetsMap.values()) {
      assertEquals(new Long(0), offset);
    }
  }

  @Test
  public void partitionsForTopic() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    List<PartitionInfo> topic1 = consumer.partitionsFor("topic1");
    assertEquals(1, topic1.size());
    assertEquals("topic1", topic1.get(0).topic());
    assertEquals(0, topic1.get(0).partition());
  }

  @Test
  public void partitionsForAllTopics() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    grpcServerRule.getServiceRegistry().addService(new PublisherImpl());

    Map<String, List<PartitionInfo>> topicPartitionsMap = consumer.listTopics();
    assertEquals(4, topicPartitionsMap.keySet().size());
    for(List<PartitionInfo> partitions: topicPartitionsMap.values()) {
      assertEquals(1, partitions.size());
      assertEquals(0, partitions.get(0).partition());
    }
  }

  @Test
  public void paused() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    List<TopicPartition> topicPartitions = Arrays.asList(new TopicPartition("topic1", 0),
        new TopicPartition("topic2", 0));

    consumer.pause(topicPartitions);
    assertEquals(2, consumer.paused().size());

    consumer.pause(topicPartitions);
    assertEquals(2, consumer.paused().size());

    consumer.resume(Collections.singletonList(new TopicPartition("topic1", 0)));
    assertEquals(1, consumer.paused().size());

    consumer.resume(Collections.singletonList(new TopicPartition("topic2", 0)));
    assertEquals(0, consumer.paused().size());
  }

  @Test
  public void pausedDoesNotPoll() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    grpcServerRule.getServiceRegistry().addService(new SubscriberGetImpl());

    String topic = "topic";
    consumer.subscribe(Collections.singletonList(topic));
    ConsumerRecords<Integer, String> poll = consumer.poll(1000);
    assertEquals(1, poll.count());

    consumer.pause(Collections.singletonList(new TopicPartition("topic", 0)));
    poll = consumer.poll(1000);
    assertEquals(0, poll.count());

    consumer.resume(Collections.singletonList(new TopicPartition("topic", 0)));
    poll = consumer.poll(1000);
    assertEquals(1, poll.count());
  }

  @Test
  public void offsetsForTimes() {
    KafkaConsumer<Integer, String> consumer = getConsumer(false);
    long millis = 150000000000L;
    TopicPartition topicPartition = new TopicPartition("topic", 0);
    Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();

    timestampsToSearch.put(topicPartition, millis);

    Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(timestampsToSearch);
    assertTrue(result.containsKey(topicPartition));
    assertEquals(millis, result.get(topicPartition).offset());
  }

  private KafkaConsumer<Integer, String> getConsumer(boolean allowesCreation) {
    Properties properties = getTestProperties(allowesCreation);
    Config configOptions = new Config(properties);
    return new KafkaConsumer<>(configOptions, grpcServerRule.getChannel(), null);
  }

  private Properties getTestProperties(boolean allowesCreation) {
    Properties properties = new Properties();
    properties.putAll(new ImmutableMap.Builder<>()
        .put("key.deserializer",
            "org.apache.kafka.common.serialization.IntegerDeserializer")
        .put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer")
        .put("max.poll.records", 500)
        .put("group.id", "groupId")
        .put("subscription.allow.create", allowesCreation)
        .put("subscription.allow.delete", false)
        .build()
    );
    return properties;
  }

  private String getTopicPrefixString(String topicName) {
    return "projects/" +   System.getenv("GOOGLE_CLOUD_PROJECT") + "/topics/" + topicName;
  }

  static class SubscriberGetImpl extends SubscriberImplBase {

    private Timestamp keptTimestamp = Timestamp.newBuilder().setSeconds(1500).build();
    private Long keptOffset = 1234567891234L;

    @Override
    public void createSubscription(Subscription request, StreamObserver<Subscription> responseObserver) {
      responseObserver.onError(new Throwable("This subscriber does not let creating subscriptions"));
    }

    @Override
    public void getSubscription(GetSubscriptionRequest request, StreamObserver<Subscription> responseObserver) {
      Subscription s = Subscription.newBuilder().setName("projects/null/subscriptions/name").setTopic("projects/null/topics/topic").build();
      responseObserver.onNext(s);
      responseObserver.onCompleted();
    }

    @Override
    public void deleteSubscription(DeleteSubscriptionRequest request, StreamObserver<Empty> responseObserver) {
      responseObserver.onError(new Throwable("This subscriber does not let deleting subscriptions"));
    }

    @Override
    public void pull(PullRequest request, StreamObserver<PullResponse> responseObserver) {
      String topic = "topic";
      Integer key = 125;
      String value = "value";
      byte[] serializedKeyBytes = new IntegerSerializer().serialize(topic, key);
      String serializedKey = new String(Base64.encodeBase64(serializedKeyBytes));
      byte[] serializedValueBytes = new StringSerializer().serialize(topic, value);

      PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
          .setPublishTime(keptTimestamp)
          .putAttributes("key", serializedKey)
          .putAttributes("offset", keptOffset.toString())
          .setData(ByteString.copyFrom(serializedValueBytes))
          .build();

      ReceivedMessage receivedMessage = ReceivedMessage.newBuilder()
          .setMessage(pubsubMessage)
          .build();

      PullResponse pullResponse = PullResponse.newBuilder()
          .addReceivedMessages(receivedMessage)
          .build();

      responseObserver.onNext(pullResponse);
      responseObserver.onCompleted();
    }

    @Override
    public void acknowledge(AcknowledgeRequest request, StreamObserver<Empty> responseObserver) {
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void seek(SeekRequest request, StreamObserver<SeekResponse> responseObserver) {
      this.keptOffset =  request.getTime().getSeconds() * 1000 + request.getTime().getNanos() / 1000;
      responseObserver.onNext(SeekResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

  }

  static class SubscriberNoExistingSubscriptionsImpl extends SubscriberImplBase {
    @Override
    public void createSubscription(Subscription request, StreamObserver<Subscription> responseObserver) {
      Subscription s = Subscription.newBuilder().setName("projects/null/subscriptions/name").setTopic("projects/null/topics/topic").build();
      responseObserver.onNext(s);
      responseObserver.onCompleted();
    }

    @Override
    public void getSubscription(GetSubscriptionRequest request, StreamObserver<Subscription> responseObserver) {
      ExecutionException executionException = new ExecutionException(new StatusRuntimeException(Status.NOT_FOUND));
      responseObserver.onError(executionException);
    }
  }

  static class PublisherImpl extends PublisherImplBase {

    @Override
    public void listTopics(ListTopicsRequest request, StreamObserver<ListTopicsResponse> responseObserver) {
      String project = "projects/" + System.getenv("GOOGLE_CLOUD_PROJECT") + "/topics/";
      List<String> topicNames = new ArrayList<>(Arrays.asList(
        project + "thisis123cat", project + "abc12345bad", project + "noWay1234", project + "funnycats000cat"));

      Builder listTopicsResponseBuilder = ListTopicsResponse.newBuilder();
      for (String topicName: topicNames) {
        listTopicsResponseBuilder.addTopicsBuilder().setName(topicName);
      }

      responseObserver.onNext(listTopicsResponseBuilder.build());
      responseObserver.onCompleted();
    }
  }

  static class ErrorSubscriberImpl extends SubscriberGetImpl {

    @Override
    public void pull(PullRequest request, StreamObserver<PullResponse> responseObserver) {
      ExecutionException executionException = new ExecutionException(new Throwable("message"));
      responseObserver.onError(executionException);
    }

    @Override
    public void acknowledge(AcknowledgeRequest request, StreamObserver<Empty> responseObserver) {
      responseObserver.onError(new Throwable("This test should not have called ack"));
    }
  }
}