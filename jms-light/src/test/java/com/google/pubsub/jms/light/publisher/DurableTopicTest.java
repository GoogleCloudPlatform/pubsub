package com.google.pubsub.jms.light.publisher;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import junit.framework.Assert;
import org.junit.Test;

/**
 * Tests for durable topics
 *
 * @author Carter Page <carter@skyscreamer.org>
 */
public class DurableTopicTest extends AbstractJMSTest {
    @Test
    public void testDurableTopic() throws JMSException {
        String durableTopicName = "testTopicSubDurable1";
        String testTopicName = "testTopic2";
        Session session = createSession();
        Topic topic = session.createTopic(testTopicName);
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, durableTopicName);
        TextMessage msg1 = session.createTextMessage("blah1 blah1 blah1");
        TextMessage msg2 = session.createTextMessage("msg2 msg2 msg2");
        TextMessage msg3 = session.createTextMessage("text3 text3 text3");
        MessageProducer producer = session.createProducer(topic);
        producer.send(msg1);
        Assert.assertEquals(msg1, subscriber.receive(2000));
        producer.send(msg2);
        producer.send(msg3);
        getConnection().close();

        Connection conn = createConnection(getConnectionFactory());
        conn.start();
        session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        subscriber = session.createDurableSubscriber(topic, durableTopicName);
        Message msgOut = subscriber.receive(2000);
        Assert.assertNotNull(msgOut);
        if (!msg2.equals(msgOut) && !msg3.equals(msgOut)) {
            Assert.fail("Got " + msgOut + " ; expected " + msg2 + " or " + msg3);
        }
        subscriber.close();
        session.unsubscribe(durableTopicName);

        boolean exceptionThrown = false;
        Message msgAfterUnsubscribe = null;
        try {
            logger.warn("EXPECTING EXCEPTION HERE:");
            subscriber = session.createDurableSubscriber(topic, durableTopicName);
            msgAfterUnsubscribe = subscriber.receive(500);
        } catch (JMSException e) {
            exceptionThrown = true;
        }
        if (!exceptionThrown && msgAfterUnsubscribe != null) {
            Assert.fail("Should not have been able to get message after unsubscribing from topic: "
                    + msgAfterUnsubscribe);
        }

        deleteTopic(conn, topic);
        conn.close();
    }

    @Test
    public void testDurableTopicDifferentClientIDs() throws JMSException {
        getConnection().close(); // Don't use the provided connection
        String durableTopicName = "testTopicSubDurable3";
        String testTopicName = "testTopic4";

        Connection conn = createConnection(getConnectionFactory());
        conn.setClientID("abc");
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic(testTopicName);
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, durableTopicName);
        TextMessage msg1 = session.createTextMessage("blah5 msg5 text5");
        MessageProducer producer = session.createProducer(topic);
        producer.send(msg1);
        conn.close();

        conn = createConnection(getConnectionFactory());
        conn.setClientID("def");
        conn.start();
        session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        subscriber = session.createDurableSubscriber(topic, durableTopicName);
        Message msgOut = subscriber.receive(2000);
        Assert.assertNull(msgOut);
        subscriber.close();
        session.unsubscribe(durableTopicName);
        conn.close();

        conn = createConnection(getConnectionFactory());
        conn.setClientID("abc");
        conn.start();
        session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        subscriber = session.createDurableSubscriber(topic, durableTopicName);
        msgOut = subscriber.receive(2000);
        Assert.assertEquals(msg1, msgOut);
        subscriber.close();
        session.unsubscribe(durableTopicName);

        deleteTopic(conn, topic);
        conn.close();
    }

    @Test
    public void testUnsubscribeWithActiveSubscriber() throws JMSException {
        String durableTopicName = "testTopicSubDurable5";
        Session session = createSession();
        Topic topic = createTempTopic(session);
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, durableTopicName);
        boolean throwsException = false;
        try {
            session.unsubscribe(durableTopicName);
        } catch(JMSException e) {
            throwsException = true;
        }

        // Clean up
        if (throwsException) {
            subscriber.close();
            session.unsubscribe(durableTopicName);
        } else {
            Assert.fail("Expected exception to be thrown when trying to unsubscribe an active topic subscription");
        }
    }

    @Test
    public void testUnsubscribeWithUnackedMsg() throws JMSException {
        String durableTopicName = "testTopicSub123";
        Session session = getConnection().createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Topic topic = createTempTopic(session);
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, durableTopicName);
        session.createProducer(topic).send(session.createMessage());
        Message message = subscriber.receive(2000); // Don't acknowledge
        boolean throwsException = false;
        try {
            session.unsubscribe(durableTopicName);
        } catch(JMSException e) {
            throwsException = true;
        }

        if (throwsException) {
            // Clean up
            message.acknowledge();
            subscriber.close();
            session.unsubscribe(durableTopicName);
        } else {
            Assert.fail("Expected exception to be thrown when trying to unsubscribe a topic with an unacked msg");
        }
    }

    @Test
    public void testDoubleSubscribe() throws JMSException {
        String durableTopicName = "testTopicSub9876";
        Session session = createSession();
        Topic topic = createTempTopic(session);
        TopicSubscriber subscriber = session.createDurableSubscriber(topic, durableTopicName);
        boolean throwsException = false;
        try {
            session.createDurableSubscriber(topic, durableTopicName);
        } catch(JMSException e) {
            throwsException = true;
        }

        if (throwsException) {
            // Clean up
            subscriber.close();
            session.unsubscribe(durableTopicName);
        } else {
            Assert.fail("Expected exception to be thrown when trying to double-subscribe an durable topic");
        }
    }
}
