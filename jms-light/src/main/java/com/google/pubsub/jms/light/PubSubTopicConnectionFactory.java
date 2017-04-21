package com.google.pubsub.jms.light;

import javax.jms.JMSException;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

/**
 * Default PubSub {@link TopicConnectionFactory} implementation.
 *
 * @author Daiqian Zhang
 */
public class PubSubTopicConnectionFactory extends PubSubConnectionFactory implements TopicConnectionFactory {

  @Override
  public TopicConnection createTopicConnection() throws JMSException {
    return null;
  }

  @Override
  public TopicConnection createTopicConnection(final String userName, final String password) throws JMSException {
    return null;
  }
}
