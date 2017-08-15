package com.google.pubsub.clients.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.ListTopicsResponse.Builder;
import com.google.pubsub.v1.PublisherGrpc.PublisherImplBase;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberImplBase;
import com.google.pubsub.v1.Subscription;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
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

  private KafkaConsumer<Integer, String> consumer;

  @Before
  public void setUp() throws ExecutionException, InterruptedException {
    ConsumerConfig config = getConsumerConfig();
    consumer = new KafkaConsumer<>(config, null,
        null, grpcServerRule.getChannel(), null);
  }

  @Test
  public void subscribe() {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());

    SubscriberFutureStub subscriberFutureStub = SubscriberGrpc.newFutureStub(grpcServerRule.getChannel());

    subscriberFutureStub.createSubscription(Subscription.newBuilder().build());
    Set<String> topics = new HashSet<>(Arrays.asList("topic1", "topic2"));
    consumer.subscribe(Arrays.asList("topic2", "topic1"));

    Set<String> subscribed = consumer.subscription();
    assertEquals(topics, subscribed);
  }

  @Test
  public void unsubscribe() {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());

    consumer.subscribe(Arrays.asList("topic2", "topic1"));
    consumer.unsubscribe();

    assertTrue(consumer.subscription().isEmpty());
  }

  @Test
  public void resubscribe() {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());

    Set<String> topics = new HashSet<>(Arrays.asList("topic4", "topic3"));

    consumer.subscribe(Arrays.asList("topic2", "topic1"));
    consumer.subscribe(Arrays.asList("topic3", "topic4"));

    Set<String> subscribed = consumer.subscription();
    assertEquals(topics, subscribed);
  }

  @Test
  public void subscribeWithRecurringTopics() {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());

    Set<String> topics = new HashSet<>(Arrays.asList("topic", "topic1"));

    consumer.subscribe(Arrays.asList("topic", "topic1", "topic"));

    Set<String> subscribed = consumer.subscription();
    assertEquals(topics, subscribed);
  }

  @Test
  public void unsubscribeSubscriptionDeleted() {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());

    Set<String> topics = new HashSet<>(Arrays.asList("topic", "topic1"));

    consumer.subscribe(Arrays.asList("topic", "topic1", "topic"));

    Set<String> subscribed = consumer.subscription();
    assertEquals(topics, subscribed);
  }

  @Test
  public void patternSubscription() {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());
    grpcServerRule.getServiceRegistry().addService(new PublisherImpl());

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
  public void deserializeProperly() throws ExecutionException, InterruptedException {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());

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
  }

  @Test
  public void pollExecutionException() throws ExecutionException, InterruptedException {
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

  private ConsumerConfig getConsumerConfig() {
    Properties properties = new Properties();
    properties.putAll(new ImmutableMap.Builder<>()
        .put("key.deserializer",
            "org.apache.kafka.common.serialization.IntegerDeserializer")
        .put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer")
        .put("max.poll.records", 500)
        .put("group.id", "groupId")
        .put("subscription.allow.create", false)
        .build()
    );

    return new ConsumerConfig(ConsumerConfig.addDeserializerToConfig(properties, null, null));
  }

  static class SubscriberImpl extends SubscriberImplBase {

    @Override
    public void createSubscription(Subscription request, StreamObserver<Subscription> responseObserver) {
      Subscription s = Subscription.newBuilder().setName("name").setTopic("projects/null/topics/topic").build();
      responseObserver.onNext(s);
      responseObserver.onCompleted();
    }

    @Override
    public void deleteSubscription(DeleteSubscriptionRequest request, StreamObserver<Empty> responseObserver) {
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void pull(PullRequest request, StreamObserver<PullResponse> responseObserver) {
      String topic = "topic";
      Integer key = 125;
      String value = "value";
      byte[] serializedKeyBytes = new IntegerSerializer().serialize(topic, key);
      String serializedKey = new String(serializedKeyBytes, StandardCharsets.UTF_8);
      byte[] serializedValueBytes = new StringSerializer().serialize(topic, value);

      Timestamp timestamp = Timestamp.newBuilder().setSeconds(1500).build();

      PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
          .setPublishTime(timestamp)
          .putAttributes("key", serializedKey)
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

  static class ErrorSubscriberImpl extends SubscriberImpl {

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