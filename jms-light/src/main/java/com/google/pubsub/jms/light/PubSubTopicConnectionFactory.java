package com.google.pubsub.jms.light;

import javax.jms.JMSException;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

/**
 * Default PubSub {@link TopicConnectionFactory} implementation.
 *
 * @author Daiqian Zhang
 */
public class PubSubTopicConnectionFactory implements TopicConnectionFactory {

  @Override
  public TopicConnection createTopicConnection() throws JMSException {
    return null;
  }

  @Override
  public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
    return null;
  }
}
