package com.google.pubsub.clients.consumer;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.pubsub.common.ChannelUtil;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberBlockingStub;
import com.google.pubsub.v1.Subscription;
import io.grpc.Channel;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Created by pietrzykp on 7/28/17.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({SubscriberGrpc.class, ChannelUtil.class})
@SuppressStaticInitializationFor("com.google.pubsub.common.ChannelUtil")
public class KafkaConsumerTest {

  private KafkaConsumer<String, Integer> consumer;
  private SubscriberBlockingStub stub;

  @Before
  public void setUp() {
    Properties properties = new Properties();
    properties.putAll(new ImmutableMap.Builder<>()
        .put("key.deserializer", "com.google.pubsub.kafkastubs.common.serialization.StringDeserializer")
        .put("value.deserializer", "com.google.pubsub.kafkastubs.common.serialization.IntegerDeserializer")
        .put("max.poll.records", 500)
        .build()
    );

    PowerMockito.mockStatic(ChannelUtil.class);
    StubCreator stubCreator = mock(StubCreator.class);
    stub = mock(SubscriberBlockingStub.class);

    Subscription s = Subscription.newBuilder().setName("name").setTopic("topic").build();

    ConsumerConfig config = new ConsumerConfig(
        ConsumerConfig.addDeserializerToConfig(properties, null, null));

    when(stubCreator.getSubscriberBlockingStub()).thenReturn(stub);
    when(stub.createSubscription(any())).thenReturn(s);


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

    verify(stub, times(2)).createSubscription(any());
  }


}
