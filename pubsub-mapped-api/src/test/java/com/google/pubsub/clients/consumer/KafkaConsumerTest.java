package com.google.pubsub.clients.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.common.ChannelUtil;
import com.google.pubsub.common.StubCreator;
import com.google.pubsub.kafkastubs.common.KafkaException;
import com.google.pubsub.kafkastubs.common.serialization.IntegerSerializer;
import com.google.pubsub.kafkastubs.common.serialization.StringSerializer;
import com.google.pubsub.kafkastubs.consumer.ConsumerRecord;
import com.google.pubsub.kafkastubs.consumer.ConsumerRecords;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherBlockingStub;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberBlockingStub;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PrepareForTest({SubscriberGrpc.class, PublisherGrpc.class, ChannelUtil.class})
@SuppressStaticInitializationFor("com.google.pubsub.common.ChannelUtil")
public class KafkaConsumerTest {

  private KafkaConsumer<Integer, String> consumer;
  private SubscriberBlockingStub blockingStub;
  private SubscriberFutureStub futureStub;
  private PublisherBlockingStub publisherBlockingStub;

  @Before
  public void setUp() throws ExecutionException, InterruptedException {
    Properties properties = new Properties();
    properties.putAll(new ImmutableMap.Builder<>()
        .put("key.deserializer", "com.google.pubsub.kafkastubs.common.serialization.IntegerDeserializer")
        .put("value.deserializer", "com.google.pubsub.kafkastubs.common.serialization.StringDeserializer")
        .put("max.poll.records", 500)
        .build()
    );

    PowerMockito.mockStatic(ChannelUtil.class);
    StubCreator stubCreator = mock(StubCreator.class);
    blockingStub = mock(SubscriberBlockingStub.class);
    futureStub = mock(SubscriberFutureStub.class);
    publisherBlockingStub = mock(PublisherBlockingStub.class);

    Subscription s = Subscription.newBuilder().setName("name").setTopic("projects/null/topics/topic").build();

    ConsumerConfig config = new ConsumerConfig(
        ConsumerConfig.addDeserializerToConfig(properties, null, null));

    when(stubCreator.getSubscriberBlockingStub()).thenReturn(blockingStub);
    when(stubCreator.getSubscriberFutureStub()).thenReturn(futureStub);
    when(stubCreator.getSubscriberFutureStub(100)).thenReturn(futureStub);
    when(stubCreator.getPublisherBlockingStub()).thenReturn(publisherBlockingStub);

    when(blockingStub.createSubscription(any())).thenReturn(s);

    ListenableFuture<Subscription> mockedSubscription = mock(ListenableFuture.class);
    when(mockedSubscription.get()).thenReturn(s);
    when(futureStub.createSubscription(any())).thenReturn(mockedSubscription);

    consumer = new KafkaConsumer<>(config, null,
        null, stubCreator);
  }

  @Test
  public void testSubscribe() {
    Set<String> topics = new HashSet<>(Arrays.asList("topic1", "topic2"));
    consumer.subscribe(Arrays.asList("topic2", "topic1"));

    Set<String> subscribed = consumer.subscription();
    assertTrue(topics.equals(subscribed));
  }

  @Test
  public void testUnsubscribe() {
    consumer.subscribe(Arrays.asList("topic2", "topic1"));
    consumer.unsubscribe();

    assertTrue(consumer.subscription().isEmpty());
  }

  @Test
  public void testResubscribe() {
    Set<String> topics = new HashSet<>(Arrays.asList("topic4", "topic3"));

    consumer.subscribe(Arrays.asList("topic2", "topic1"));
    consumer.subscribe(Arrays.asList("topic3", "topic4"));

    Set<String> subscribed = consumer.subscription();
    assertTrue(topics.equals(subscribed));
  }

  @Test
  public void testSubscribeWithRecurringTopics() {
    Set<String> topics = new HashSet<>(Arrays.asList("topic", "topic1"));

    consumer.subscribe(Arrays.asList("topic", "topic1", "topic"));

    Set<String> subscribed = consumer.subscription();
    assertTrue(topics.equals(subscribed));

    verify(futureStub, times(2)).createSubscription(any());
  }

  @Test
  public void testUnsubscribeSubscriptionDeleted() {
    Set<String> topics = new HashSet<>(Arrays.asList("topic", "topic1"));

    consumer.subscribe(Arrays.asList("topic", "topic1", "topic"));

    Set<String> subscribed = consumer.subscription();
    assertTrue(topics.equals(subscribed));

    verify(futureStub, times(2)).createSubscription(any());
  }

  @Test
  public void testPatternSubscription() {
    List<String> topicNames = new ArrayList<>(Arrays.asList("projects/null/topics/thisis123cat", "projects/null/topics/abc12345bad", "projects/null/topics/noWay1234", "projects/null/topics/funnycats000cat"));
    List<Topic> topics = new ArrayList<>();

    for(String topicName: topicNames) {
      topics.add(Topic.newBuilder().setName(topicName).build());
    }

    ListTopicsResponse listTopicsResponse = ListTopicsResponse.newBuilder()
        .addAllTopics(topics).build();

    when(publisherBlockingStub.listTopics(any())).thenReturn(listTopicsResponse);
    Pattern pattern = Pattern.compile("[a-z]*\\d{3}cat");

    consumer.subscribe(pattern, null);
    Set<String> subscribed = consumer.subscription();
    Set<String> expectedTopics = new HashSet<>(Arrays.asList("thisis123cat", "funnycats000cat"));
    assertEquals(expectedTopics, subscribed);
  }

  @Test
  public void emptyPatternFails() {
    try {
      Pattern pattern = null;
      consumer.subscribe(pattern, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Topic pattern to subscribe to cannot be null", e.getMessage());
    }
  }

  @Test
  public void nullTopicListFails() {
    try {
      List<String> topics = null;
      consumer.subscribe(topics);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Topic collection to subscribe to cannot be null", e.getMessage());
    }
  }

  @Test
  public void emptyTopicFails() {
    try {
      List<String> topics = new ArrayList<>(Collections.singletonList("    "));
      consumer.subscribe(topics);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Topic collection to subscribe to cannot contain null or empty topic", e.getMessage());
    }
  }

  @Test
  public void noSubscriptionsPollFails() {
    try {
      consumer.poll(100);
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Consumer is not subscribed to any topics", e.getMessage());
    }
  }

  @Test
  public void negativePollTimeoutFails() {
    try {
      consumer.poll(-200);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Timeout must not be negative", e.getMessage());
    }
  }


  @Test
  public void testDeserializeProperly() throws ExecutionException, InterruptedException {
    String topic = "topic";
    consumer.subscribe(Collections.singletonList(topic));
    Integer key = 125;
    String value = "value";
    byte[] serializedKeyBytes = new IntegerSerializer().serialize(topic, key);
    String serializedKey = new String(serializedKeyBytes, StandardCharsets.UTF_8);
    byte[] serializedValueBytes = new StringSerializer().serialize(topic, value);


    Timestamp timestamp = Timestamp.newBuilder().setSeconds(1500).build();

    PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
        .setPublishTime(timestamp)
        .putAttributes("key_attribute", serializedKey)
        .setData(ByteString.copyFrom(serializedValueBytes))
        .build();

    ReceivedMessage receivedMessage = ReceivedMessage.newBuilder().setMessage(pubsubMessage).build();
    PullResponse pullResponse = PullResponse.newBuilder()
        .addReceivedMessages(receivedMessage)
        .build();

    ListenableFuture<PullResponse> pullResponseListenableFuture = mock(ListenableFuture.class);
    when(pullResponseListenableFuture.get()).thenReturn(pullResponse);

    when(futureStub.pull(any())).thenReturn(pullResponseListenableFuture);
    ConsumerRecords<Integer, String> consumerRecord = consumer.poll(100);

    assertEquals(consumerRecord.count(), 1);
    Iterable<ConsumerRecord<Integer, String>> recordsForTopic = consumerRecord.records("topic");

    ConsumerRecord<Integer, String> next = recordsForTopic.iterator().next();

    assertEquals(value, next.value());
    assertEquals(key, next.key());

    verify(blockingStub, times(1)).acknowledge(any());
  }

  @Test
  public void testPollExecutionException() throws ExecutionException, InterruptedException {
    String topic = "topic";
    consumer.subscribe(Collections.singletonList(topic));
    ExecutionException executionException = new ExecutionException("message", new Throwable());

    ListenableFuture<PullResponse> pullResponseListenableFuture = mock(ListenableFuture.class);
    when(pullResponseListenableFuture.get()).thenThrow(executionException);
    when(futureStub.pull(any())).thenReturn(pullResponseListenableFuture);

    try {
      consumer.poll(100);
      fail();
    } catch (KafkaException e) {
      assertEquals("java.util.concurrent.ExecutionException: message", e.getMessage());
    }

    verify(blockingStub, times(0)).acknowledge(any());
  }

}