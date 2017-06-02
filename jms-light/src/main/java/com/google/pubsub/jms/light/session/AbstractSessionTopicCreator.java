package com.google.pubsub.jms.light.session;

import com.google.pubsub.jms.light.PubSubConnection;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;

/**
 * Default implementation of {@link javax.jms.Session} topic creations. 
 *
 * @author Maksym Prokhorenko
 */
public abstract class AbstractSessionTopicCreator extends AbstractSessionMessageCreator {
  /**
   * Default constructor.
   * @param connection is a jms connection.
   * @param transacted is an indicator whether the session in transacted mode.
   * @param acknowledgeMode is an acknowledgement mode {@link javax.jms.Session#AUTO_ACKNOWLEDGE},
   *        {@link javax.jms.Session#CLIENT_ACKNOWLEDGE},
   *        {@link javax.jms.Session#SESSION_TRANSACTED}.
   */
  public AbstractSessionTopicCreator(
      final PubSubConnection connection,
      final boolean transacted,
      final int acknowledgeMode) {
    super(connection, transacted, acknowledgeMode);
  }

  @Override
  public Topic createTopic(final String topicName) throws JMSException {
    return null;
  }

  @Override
  public TemporaryTopic createTemporaryTopic() throws JMSException {
    return null;
  }
}
