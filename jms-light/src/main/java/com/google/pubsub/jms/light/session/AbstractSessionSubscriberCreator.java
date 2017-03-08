package com.google.pubsub.jms.light.session;

import com.google.pubsub.jms.light.PubSubConnection;

import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

/**
 * Default implementation of {@link javax.jms.Session} subscriber creations. 
 *
 * @author Maksym Prokhorenko
 */
public abstract class AbstractSessionSubscriberCreator extends AbstractSessionProducerCreator
{
  /**
   * Default constructor.
   * @param connection is a jms connection.
   * @param transacted is an indicator whether the session in transacted mode.
   * @param acknowledgeMode is an acknowledgement mode {@link javax.jms.Session#AUTO_ACKNOWLEDGE}, {@link javax.jms.Session#CLIENT_ACKNOWLEDGE},
   * {@link javax.jms.Session#SESSION_TRANSACTED}.
   */
  public AbstractSessionSubscriberCreator(final PubSubConnection connection, final boolean transacted, final int acknowledgeMode)
  {
    super(connection, transacted, acknowledgeMode);
  }

  @Override
  public TopicSubscriber createDurableSubscriber(final Topic topic, final String name) throws JMSException
  {
    return null;
  }

  @Override
  public TopicSubscriber createDurableSubscriber(final Topic topic,
                                                 final String name,
                                                 final String messageSelector,
                                                 final boolean noLocal) throws JMSException
  {
    return null;
  }

}
