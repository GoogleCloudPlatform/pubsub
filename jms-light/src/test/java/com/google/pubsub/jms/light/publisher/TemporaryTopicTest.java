package com.google.pubsub.jms.light.publisher;

import com.google.pubsub.jms.light.PubSubConnectionFactory;

import java.util.Collection;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.InvalidDestinationException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test temporary topics
 *
 * @author Carter Page <carter@skyscreamer.org>
 */
public class TemporaryTopicTest extends AbstractJMSTest {
  @Test
  public void testTemporaryTopic() throws Exception {
    Session session = createSession();
    TemporaryTopic temporaryTopic = session.createTemporaryTopic();
    Assert.assertFalse(temporaryTopic.getTopicName().endsWith("null"));
    TextMessage testMessage = session.createTextMessage("blah blah blah");
    MessageProducer producer = session.createProducer(temporaryTopic);
    MessageConsumer consumer = session.createConsumer(temporaryTopic);
    producer.send(testMessage);
    Message msgOut = consumer.receive(5000);
    Assert.assertTrue(msgOut instanceof TextMessage);
    Assert.assertEquals("Message body not equal", testMessage.getText(), ((TextMessage) msgOut).getText());
  }

  @Test(expected = InvalidDestinationException.class)
  public void testTemporaryQueueAcrossConnections() throws Exception {
    Session session = createSession();
    TemporaryTopic temporaryTopic = session.createTemporaryTopic();
    Connection theWrongConnection = createConnection(getConnectionFactory());
    Session theWrongSession = theWrongConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    theWrongSession.createConsumer(temporaryTopic);
  }

  @Test
  public void testTemporaryTopicSuffix() throws Exception {
    // TODO: figure out why in the original tests a reference to a connection factory was used
    ConnectionFactory connectionFactory = new PubSubConnectionFactory(/*getConnectionFactory()*/);
    String temporaryTopicSuffix = UUID.randomUUID().toString();
    Assert.assertTrue(temporaryTopicSuffix.length() > 0);
    // TODO: Figure out what this is used for an implement it
    //connectionFactory.setTemporaryTopicSuffix(temporaryTopicSuffix);
    Connection connection = createConnection(connectionFactory);
    connection.start();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    TemporaryTopic topic = session.createTemporaryTopic();
    Assert.assertTrue(topic.getTopicName().endsWith(temporaryTopicSuffix));
    connection.close();
  }

  @Test
  public void testDeleteUnusedTemporaryTopics() throws Exception {
    Connection conn1;
    String suffix1;
    Connection conn2;
    String suffix2;

    {
      // TODO: figure out why in the original tests a reference to a connection factory was used
      ConnectionFactory connectionFactory = new PubSubConnectionFactory(/*getConnectionFactory()*/);
      suffix1 = UUID.randomUUID().toString();
      // TODO: Figure out what this is used for an implement it
      //connectionFactory.setTemporaryTopicSuffix(suffix1);
      conn1 = createConnection(connectionFactory);
    }
    {
      // TODO: figure out why in the original tests a reference to a connection factory was used
      ConnectionFactory connectionFactory = new PubSubConnectionFactory(/*getConnectionFactory()*/);
      suffix2 = UUID.randomUUID().toString();
      // TODO: Figure out what this is used for an implement it
      //connectionFactory.setTemporaryTopicSuffix(suffix2);
      conn2 = createConnection(connectionFactory);
    }

    try {
      conn1.start();
      conn2.start();
      TemporaryTopic topic1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE).createTemporaryTopic();
      TemporaryTopic topic2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE).createTemporaryTopic();
      Assert.assertTrue(containsTemporaryTopic(conn1, topic1));
      Assert.assertTrue(containsTemporaryTopic(conn2, topic2));
      deleteTemporaryTopic(conn1, suffix2 + "X");
      Assert.assertTrue(containsTemporaryTopic(conn1, topic1));
      Assert.assertTrue(containsTemporaryTopic(conn2, topic2));
      deleteTemporaryTopic(conn1, suffix2);
      Assert.assertTrue(containsTemporaryTopic(conn1, topic1));
      Assert.assertFalse(containsTemporaryTopic(conn2, topic2));
    } finally {
      try {
        conn1.close();
      } catch (Throwable t) {
        logger.error("Unable to close connection 1", t);
      }
      try {
        conn2.close();
      } catch (Throwable t) {
        logger.error("Unable to close connection 1", t);
      }
    }
  }

  // Because the queues returned by SNS ListTopics is not synchronous with creation and deletion of topics, it is
  // too flaky to test in a quick, automated fashion.  This could be done with thie very slow test
  // but we'll leave it disabled so our overall suite can remain fast.
  @Test
  public void testTemporaryTopicDeletion() throws Exception {
    Session session = createSession();
    TemporaryTopic temporaryTopic = session.createTemporaryTopic();
    Collection<TemporaryTopic> allTemporaryTopics = listAllTemporaryTopics(getConnection());
    Assert.assertTrue("Temporary topic should exist", allTemporaryTopics.contains(temporaryTopic));
    getConnection().close();
    allTemporaryTopics = listAllTemporaryTopics(getConnection());
    Assert.assertFalse("Temporary topic should not exist", allTemporaryTopics.contains(temporaryTopic));
  }
}
