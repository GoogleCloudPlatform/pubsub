package com.google.pubsub.jms.light;

import com.google.pubsub.jms.light.destination.PubSubTemporaryTopic;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;

/**
 * Default PubSub {@link javax.jms.TopicSession} implementation.
 *
 * @author Daiqian Zhang
 */
class PubSubTopicSession extends PubSubSession {

  /**
   * Default constructor.
   * @param connection is a jms connection.
   * @param transacted is an indicator whether the session in transacted mode.
   * @param acknowledgeMode is an acknowledgement mode {@link javax.jms.Session#AUTO_ACKNOWLEDGE},
   *        {@link javax.jms.Session#CLIENT_ACKNOWLEDGE},
   *        {@link javax.jms.Session#SESSION_TRANSACTED}.
   */
  public PubSubTopicSession(
      final PubSubConnection connection,
      final boolean transacted,
      final int acknowledgeMode) {
    super(connection, transacted, acknowledgeMode);
  }

  @Override
  public TemporaryTopic createTemporaryTopic() {
    return new PubSubTemporaryTopic(generateTemporaryTopicName());
  }

  @Override
  public void unsubscribe(final String name) {
  }

  @Override
  public QueueBrowser createBrowser(final Queue queue) throws JMSException {
    throw new JMSException("createBrowser can not be used in Pub/Sub messaging domain.");
  }

  @Override
  public Queue createQueue(final String queueName) throws JMSException {
    throw new JMSException("createQueue can not be used in Pub/Sub messaging domain.");
  }

  @Override
  public TemporaryQueue createTemporaryQueue() throws JMSException {
    throw new JMSException("createTemporaryQueue can not be used in Pub/Sub messaging domain.");
  }

  private String generateTemporaryTopicName() {
    // TODO make sure the generated string is unique to the connection.
    return null;
  }
}
