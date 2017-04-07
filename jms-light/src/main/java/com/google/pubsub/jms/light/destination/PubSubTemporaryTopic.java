package com.google.pubsub.jms.light.destination;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;

/**
 * Default PubSub {@link TemporaryTopic} implementation.
 *
 * @author Xiao (Frank) Yang
 */
public class PubSubTemporaryTopic  extends PubSubDestination implements TemporaryTopic {
  private final String topicName;

  /**
   * Default PubSub Topic constructor.
   * @param topicName is a pubsub topic name.
   */
  public PubSubTemporaryTopic(final String topicName) {
    super();
    this.topicName = topicName;
  }
  
  @Override
  public String getTopicName() throws JMSException {
    return topicName;
  }

  @Override
  public void delete() throws JMSException {
    // TODO: Throw JmsException if there are still subscribers on this topic.
    // Waiting for the implementation of PubSubTopicSubscriber class.
  }
}
