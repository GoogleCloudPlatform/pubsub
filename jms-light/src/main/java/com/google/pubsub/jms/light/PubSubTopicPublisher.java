package com.google.pubsub.jms.light;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

/**
 * Default PubSub {@link TopicPublisher} implementation.
 *
 * @author Maksym Prokhorenko
 */
public class PubSubTopicPublisher extends PubSubMessageProducer implements TopicPublisher {

  /**
   * Default constructor.
   * @param session is a jms session {@link javax.jms.Session}.
   * @param topic is a jms topic.
   * @throws JMSException in case {@link PubSubMessageProducer} doesn't support destination
   *         or destination object fails to return topic/queue name.
   */
  public PubSubTopicPublisher(final PubSubSession session, final Topic topic) throws JMSException {
    super(session, topic);
  }

  @Override
  public Topic getTopic() throws JMSException {
    return (Topic) getDestination();
  }

  @Override
  public void publish(final Message message) throws JMSException {
    publish(getTopic(), message, getDeliveryMode(), getPriority(), getTimeToLive());
  }

  @Override
  public void publish(final Topic topic, final Message message) throws JMSException {
    publish(topic, message, getDeliveryMode(), getPriority(), getTimeToLive());
  }

  @Override
  public void publish(
      final Message message,
      final int deliveryMode,
      final int priority,
      final long timeToLive) throws JMSException {
    publish(getTopic(), message, deliveryMode, priority, timeToLive);
  }

  @Override
  public void publish(
      final Topic topic,
      final Message message,
      final int deliveryMode,
      final int priority,
      final long timeToLive) throws JMSException {
    send(topic, message, deliveryMode, priority, timeToLive);
  }
}
