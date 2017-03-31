package com.google.pubsub.jms.light.publisher;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;

public abstract class AbstractJMSTest {
  protected final Log logger = LogFactory.getLog(getClass());

  @Before
  public void setUp() throws JMSException, IOException {
    // TODO: Set up at least a session
  }

  protected Connection createConnection(ConnectionFactory connectionFactory) throws JMSException {
    // TODO: create connection
    return null;
  }

  protected QueueConnection createQueueConnection(
      QueueConnectionFactory connectionFactory)
          throws JMSException{
    return null;
  }

  protected TopicConnection createTopicConnection(
      TopicConnectionFactory connectionFactory)
          throws JMSException {
    // TODO: Create Topic Connection
    return null;
  }

  protected Message sendAndReceive(Message msg) throws JMSException {
    Session session = createSession();
    Queue testQueue = createTempQueue(session);
    session.createProducer(testQueue).send(msg);
    Message msgOut = createSession().createConsumer(testQueue).receive();
    Assert.assertNotNull("Got null message back", msgOut);
    msgOut.acknowledge();
    return msgOut;
  }

  @After
  public void tearDown() throws JMSException {
    // TODO: check for exceptions, close session
  }

  public ConnectionFactory getConnectionFactory() {
    // TODO: need a reference to a connection factory
    return null;
  }

  protected Connection getConnection() {
    // TODO: need a reference to a default connection
    return null;
  }

  protected Session createSession() throws JMSException {
    // TODO: Create new session
    return null;
  }

  protected TemporaryQueue createTempQueue(Session session) throws JMSException {
    return session.createTemporaryQueue();
  }

  protected TemporaryTopic createTempTopic(Session session) throws JMSException {
    return session.createTemporaryTopic();
  }

  protected void deleteQueue(Queue queue) throws JMSException {
    // TODO: see what deleteQueue is supposed to do and implement it
    //getConnection().deleteQueue(queue);
  }

  protected void compareTextMessages(
      TextMessage[] expectedTextMessages,
      TextMessage[] actualTextMessages)
          throws JMSException {
    if (expectedTextMessages == null) {
      throw new NullPointerException("Expected text messages cannot be null");
    }
    junit.framework.Assert.assertNotNull("Actual text message array null", actualTextMessages);
    junit.framework.Assert.assertEquals(
        "Expected text message array size does not equal actual",
        expectedTextMessages.length,
        actualTextMessages.length);
    Map<String, Integer> expectedTextCount = countTexts(expectedTextMessages);
    Map<String, Integer> actualTextCount = countTexts(expectedTextMessages);
    junit.framework.Assert.assertEquals(
        "Compare expected and actual text messages",
        expectedTextCount,
        actualTextCount);
  }

  private Map<String, Integer> countTexts(TextMessage[] expectedTextMessages) throws JMSException {
    Map<String, Integer> textCount = new HashMap<String, Integer>();
    for (TextMessage textMessage : expectedTextMessages) {
      String text = textMessage.getText();
      int count = textCount.containsKey(text) ? textCount.get(text) : 0;
      ++count;
      textCount.put(text, count);
    }
    return textCount;
  }

  protected void breakSession(Session session) {
    // TODO: once we have a reference to a session, need to break the session
    //session.setBreakSessionForTesting(true);
  }
  
  protected boolean containsTemporaryTopic(Connection conn, TemporaryTopic topic) {
    // TODO: Verify topic list
    return false;
  }
  
  protected void deleteTemporaryTopic(String topicName) {
    deleteTemporaryTopic(getConnection(), topicName);
  }
  
  protected void deleteTemporaryTopic(Connection conn, String topicName) {
    // TODO: delete temporary topic
  }
  
  protected Collection<TemporaryTopic> listAllTemporaryTopics(Connection conn) {
    // TODO: list all temporary topics
    return null;
  }
  
  protected void deleteTopic(Topic topic) {
    deleteTopic(getConnection(), topic);
  }
  
  protected void deleteTopic(Connection conn, Topic topic) {
    // TODO: delete topic
  }
}