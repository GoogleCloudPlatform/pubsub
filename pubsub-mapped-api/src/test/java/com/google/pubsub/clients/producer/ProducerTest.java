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

package com.google.pubsub.clients.producer;

import org.junit.Test;
import org.junit.Before;
import org.junit.Assert;
import org.junit.runner.RunWith;

import org.mockito.Matchers;
import org.mockito.ArgumentCaptor;

import com.google.pubsub.v1.TopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.Publisher.Builder;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.batching.BatchingSettings;

import com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import com.google.api.core.ApiFuture;

import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;
import static org.powermock.api.mockito.PowerMockito.when;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Publisher.class, TopicAdminClient.class})
public class ProducerTest {

  private ApiFuture lf;
  private Publisher stub;
  private Builder stubBuilder;
  private ProducerConfig config;
  private Serializer serializer;
  private Deserializer deserializer;
  private TopicAdminClient topicAdmin;
  private ArgumentCaptor<PubsubMessage> captor;
  private KafkaProducer<String, Integer> publisher;

  @Before
  public void setUp() throws IOException {
    Properties properties = new Properties();
    properties.putAll(new ImmutableMap.Builder<>()
        .put("acks", "1")
        .put("project", "project")
        .put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
        .build()
    );

    lf = PowerMockito.mock(ApiFuture.class);
    captor = ArgumentCaptor.forClass(PubsubMessage.class);

    stub = PowerMockito.mock(Publisher.class, Mockito.RETURNS_DEEP_STUBS);
    stubBuilder = PowerMockito.mock(Builder.class, Mockito.RETURNS_DEEP_STUBS);
    topicAdmin = PowerMockito.mock(TopicAdminClient.class, Mockito.RETURNS_DEEP_STUBS);

    config = new ProducerConfig(
        ProducerConfig.addSerializerToConfig(properties, null, null));

    PowerMockito.mockStatic(Publisher.class);
    PowerMockito.mockStatic(TopicAdminClient.class);

    when(stubBuilder.build()).thenReturn(stub);
    when(stub.publish(captor.capture())).thenReturn(lf);
    when(TopicAdminClient.create()).thenReturn(topicAdmin);
    when(Publisher.defaultBuilder(Matchers.<TopicName>any())).thenReturn(stubBuilder);
    when(stubBuilder.setRetrySettings(Matchers.<RetrySettings>any())).thenReturn(stubBuilder);
    when(stubBuilder.setBatchingSettings(Matchers.<BatchingSettings>any())).thenReturn(stubBuilder);

    publisher = new KafkaProducer<String, Integer>(config, null, null);
  }

  @Test
  public void testFlush() {
    publisher.send(new ProducerRecord<String, Integer>("topic", 123));

    publisher.flush();

    publisher.send(new ProducerRecord<String, Integer>("topic", 456));

    try {
      Mockito.verify(stub, Mockito.times(1)).shutdown();

      Mockito.verify(stub, Mockito.times(2))
          .publish(Matchers.<PubsubMessage>any());

      Mockito.verify(stubBuilder, Mockito.times(2)).build();
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  @Test
  public void testCallback() {
    serializer = new IntegerSerializer();

    Callback cb = new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        Assert.assertEquals(metadata.topic(), "topic");

        Assert.assertEquals(metadata.serializedKeySize(), 0);

        Assert.assertEquals(metadata.serializedValueSize(),
            serializer.serialize("topic", 123).length);
      }
    };

    publisher.send(new ProducerRecord<String, Integer>("topic", 123), cb);
  }

  @Test
  public void testSerializers() {
    publisher.send(new ProducerRecord<String, Integer>("topic", 123));

    deserializer = new StringDeserializer();

    Assert.assertEquals("Key should be an empty string.",
        "", deserializer.deserialize("topic",
            captor.getValue().getAttributesMap().get("key").getBytes()));

    deserializer = new IntegerDeserializer();

    Assert.assertEquals("Value should be the one previously provided.",
        123, deserializer.deserialize("topic",
            captor.getValue().getData().toByteArray()));
  }

  @Test (expected = NullPointerException.class)
  public void testPublishNull() {
    publisher.send(new ProducerRecord<String, Integer>("topic", null));
  }

  @Test (expected = IllegalArgumentException.class)
  public void testNegativeTimeout() {
    publisher.close(-1, TimeUnit.SECONDS);
  }

  @Test
  public void testCloseOnCompletion() {
    publisher.send(new ProducerRecord<String, Integer>("topic", 123));

    publisher.close();

    try {
      publisher.send(new ProducerRecord<String, Integer>("topic", 123));
    } catch (Exception e) {}

    try {
      Mockito.verify(stub, Mockito.times(1)).shutdown();

      Mockito.verify(stub, Mockito.times(1))
          .publish(Matchers.<PubsubMessage>any());
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  @Test (expected = NullPointerException.class)
  public void testEmptyRecordPublish() {
    publisher.send(null);
  }

  @Test (expected = NullPointerException.class)
  public void testPublishEmptyMessage() {
    KafkaProducer<String, String> pub = new KafkaProducer<String, String>(config,
        null, new StringSerializer());
    pub.send(new ProducerRecord<String, String>("topic", ""));
  }

  @Test
  public void testNumberOfPublishIssued() {
    publisher.send(new ProducerRecord<String, Integer>("topic", 123));

    Mockito.verify(stub, Mockito.times(1))
        .publish(Matchers.<PubsubMessage>any());

    publisher.send(new ProducerRecord<String, Integer>("topic", 456));

    Mockito.verify(stub, Mockito.times(2))
        .publish(Matchers.<PubsubMessage>any());
  }

  @Test (expected = RuntimeException.class)
  public void testPublishToClosedPublisher() {
    publisher.close();

    publisher.send(new ProducerRecord<String, Integer>("topic", 123));
  }
}
