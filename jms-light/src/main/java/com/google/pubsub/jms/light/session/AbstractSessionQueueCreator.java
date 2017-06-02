package com.google.pubsub.jms.light.session;

import com.google.pubsub.jms.light.PubSubConnection;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.TemporaryQueue;

/**
 * Default implementation of {@link javax.jms.Session} queue creations. 
 *
 * @author Maksym Prokhorenko
 */
public abstract class AbstractSessionQueueCreator extends AbstractSessionTopicCreator {
  /**
   * Default constructor.
   * @param connection is a jms connection.
   * @param transacted is an indicator whether the session in transacted mode.
   * @param acknowledgeMode is an acknowledgement mode {@link javax.jms.Session#AUTO_ACKNOWLEDGE},
   *        {@link javax.jms.Session#CLIENT_ACKNOWLEDGE},
   *        {@link javax.jms.Session#SESSION_TRANSACTED}.
   */
  public AbstractSessionQueueCreator(
      final PubSubConnection connection,
      final boolean transacted,
      final int acknowledgeMode) {
    super(connection, transacted, acknowledgeMode);
  }

  @Override
  public Queue createQueue(final String queueName) throws JMSException {
    return null;
  }

  @Override
  public TemporaryQueue createTemporaryQueue() throws JMSException {
    return null;
  }
}
