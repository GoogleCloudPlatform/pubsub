package com.google.pubsub.jms.light.publisher;

import com.google.pubsub.jms.light.PubSubConnection;
import com.google.pubsub.jms.light.PubSubConnectionFactory;
import com.google.pubsub.jms.light.PubSubSession;
import com.google.pubsub.jms.light.PubSubMessageConsumer;
import com.google.pubsub.jms.light.PubSubMessageProducer;
import com.google.pubsub.jms.light.PubSubTopicConnection;
import com.google.pubsub.jms.light.PubSubTopicSession;
import com.google.pubsub.jms.light.destination.PubSubDestination;
import com.google.pubsub.jms.light.destination.PubSubTopicDestination;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import junit.framework.Assert;
import org.junit.Test;

/**
 * General topic checks.
 * 
 * These tests are based on the tests here:
 * https://github.com/skyscreamer/nevado/blob/master/nevado-jms/src/test/java/org/skyscreamer/nevado/jms/destination/GeneralTopicTest.java
 *
 * @author Carter Page <carter@skyscreamer.org>
 */
public class GeneralTopicTest extends AbstractJMSTest {
    @Test
    public void testCommonAndPubSubAreSameImplementation() {
        Assert.assertTrue(ConnectionFactory.class.isAssignableFrom(PubSubConnectionFactory.class));
        Assert.assertTrue(TopicConnectionFactory.class.isAssignableFrom(PubSubConnectionFactory.class));
        Assert.assertTrue(Connection.class.isAssignableFrom(PubSubConnection.class));
        Assert.assertTrue(TopicConnection.class.isAssignableFrom(PubSubTopicConnection.class));
        Assert.assertTrue(PubSubConnection.class.isAssignableFrom(PubSubTopicConnection.class));
        Assert.assertTrue(Topic.class.isAssignableFrom(PubSubTopicDestination.class));
        Assert.assertTrue(PubSubDestination.class.isAssignableFrom(PubSubTopicDestination.class));
        Assert.assertTrue(Destination.class.isAssignableFrom(PubSubDestination.class));
        Assert.assertTrue(Session.class.isAssignableFrom(PubSubSession.class));
        Assert.assertTrue(TopicSession.class.isAssignableFrom(PubSubTopicSession.class));
        Assert.assertTrue(PubSubSession.class.isAssignableFrom(PubSubTopicSession.class));
        Assert.assertTrue(MessageProducer.class.isAssignableFrom(PubSubMessageProducer.class));
        Assert.assertTrue(TopicPublisher.class.isAssignableFrom(PubSubMessageProducer.class));
        Assert.assertTrue(MessageConsumer.class.isAssignableFrom(PubSubMessageConsumer.class));
        Assert.assertTrue(TopicSubscriber.class.isAssignableFrom(PubSubMessageConsumer.class));
    }

    @Test
    public void testTopicFacilities() throws JMSException {
        // TODO: Create a TopicConnectionFactory
        TopicConnectionFactory connectionFactory = null;
        TopicConnection connection = createTopicConnection(connectionFactory);
        connection.start();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TemporaryTopic topic = session.createTemporaryTopic();
        TopicPublisher sender = session.createPublisher(topic);
        TopicSubscriber receiver = session.createSubscriber(topic);
        TextMessage testMessage = session.createTextMessage("1234");
        sender.send(testMessage);
        Message msgOut = receiver.receive(1000);
        Assert.assertNotNull(msgOut);
        Assert.assertTrue(msgOut instanceof TextMessage);
        Assert.assertEquals(testMessage, (TextMessage)msgOut);
        connection.close();
    }
}
