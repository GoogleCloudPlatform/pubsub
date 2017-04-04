package com.google.pubsub.jms.light.destination;

import javax.jms.JMSException;
import javax.jms.Topic;

/**
 * Default PubSub {@link Topic} implementation.
 *
 * @author Maksym Prokhorenko
 */
public class PubSubTopicDestination extends PubSubDestination implements Topic {
  private final String topicName;

  /**
   * Default PubSub Topic constructor.
   * @param topicName is a pubsub topic name.
   */
  public PubSubTopicDestination(final String topicName) {
    super();
    this.topicName = topicName;
  }

  @Override
  public String getTopicName() throws JMSException {
    return topicName;
  }
}
