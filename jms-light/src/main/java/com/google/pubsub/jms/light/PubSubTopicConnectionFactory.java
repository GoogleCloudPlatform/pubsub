package com.google.pubsub.jms.light;

import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

/**
 * Default pubSub {@link TopicConnectionFactory} implementation.
 *
 * @author Daiqian Zhang
 */
public class PubSubTopicConnectionFactory implements TopicConnectionFactory {

  @Override
  TopicConnection createTopicConnection() throws JMSException {
    return null;
  }

  @Override
  TopicConnection createTopicConnection(String userName, String password) throws JMSException {
    return null;
  }
}
