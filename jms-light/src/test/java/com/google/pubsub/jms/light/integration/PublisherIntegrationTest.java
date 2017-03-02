package com.google.pubsub.jms.light.integration;

import com.google.api.gax.grpc.FixedChannelProvider;
import com.google.api.gax.grpc.InstantiatingExecutorProvider;
import com.google.api.gax.grpc.ProviderManager;
import com.google.cloud.pubsub.deprecated.Message;
import com.google.cloud.pubsub.deprecated.PubSub;
import com.google.common.collect.Lists;
import com.google.pubsub.jms.light.PubSubConnectionFactory;
import com.google.pubsub.jms.light.destination.PubSubTopicDestination;
import io.grpc.ManagedChannelBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

@RunWith(MockitoJUnitRunner.class)
public class PublisherIntegrationTest extends BaseIntegrationTest
{
  private static final Logger LOGGER = Logger.getLogger(PublisherIntegrationTest.class.getName());
  private List<String> toSend = Lists.newArrayList(
      "\"Mystery creates wonder and wonder is the basis of man's desire to understand.\" -- Neil Armstrong",
      "\"A-OK full go.\" -- Alan B. Shepard",
      "\"Houston, Tranquillity Base here. The Eagle has landed.\" -- Neil Armstrong"
  );

  private ConnectionFactory connectionFactory = new PubSubConnectionFactory();
  private Topic topic = new PubSubTopicDestination(String.format("projects/%s/topics/%s", PROJECT_ID, TOPIC_NAME));

  @Before
  public void setUp() throws IOException
  {
    final PubSubConnectionFactory factory = (PubSubConnectionFactory) connectionFactory;
    factory.setProviderManager(createProviderManager());
  }

  /**
   * Simple producer test. Creates JMS connection/session/producer/message and send it to PubSub.
   */
  @Test
  public void sunnyDayPublish() throws JMSException, IOException
  {
    try (final Connection connection = connectionFactory.createConnection())
    {
      final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageProducer producer = session.createProducer(topic);
      for (final String text : toSend)
      {
        final TextMessage wowMessage = session.createTextMessage(text);
        producer.send(wowMessage);
      }
    }

    // verify
    // Configs PubSub's subscriber and message processor. Not the JMS one.
    final WowMessageProcessor messageProcessor = new WowMessageProcessor();
    getServiceSubscription().pullAsync(messageProcessor);

    // wait till all messages received.
    await().atMost(1, SECONDS).until(
        new Callable<List<Message>>()
        {
          @Override public List<Message> call() throws Exception {return messageProcessor.getReceivedMessages();}
        },
        hasSize(greaterThanOrEqualTo(toSend.size())));
  }

  private ProviderManager createProviderManager()
  {
    return ProviderManager.newBuilder()
        .setChannelProvider(
            FixedChannelProvider.create(
                ManagedChannelBuilder.forAddress(getServiceHost(), getServicePort()).usePlaintext(true).build()))
        .setExecutorProvider(InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(1).build())
        .build();
  }

  static class WowMessageProcessor implements PubSub.MessageProcessor
  {
    final List<Message> received = Lists.newArrayList();

    @Override
    public void process(final Message message) throws Exception
    {
      received.add(message);

      LOGGER.info(
          String.format("Received: [id: %s, payload: %s]", message.getId(), message.getPayloadAsString()));
    }

    List<Message> getReceivedMessages()
    {
      return Collections.unmodifiableList(received);
    }
  }
}
