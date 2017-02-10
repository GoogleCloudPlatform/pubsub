package com.google.pubsub.jms.light.session;

import com.google.pubsub.jms.light.PubSubConnection;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Topic;

/**
 * Default implementation of {@link javax.jms.Session} consumer creations. 
 *
 * @author Maksym Prokhorenko
 */
public abstract class AbstractSessionConsumerCreator extends AbstractSessionSubscriberCreator
{
  /**
   * Default constructor.
   * @param connection is a jms connection.
   * @param transacted is an indicator whether the session in transacted mode.
   * @param acknowledgeMode is an acknowledgement mode {@link javax.jms.Session#AUTO_ACKNOWLEDGE}, {@link javax.jms.Session#CLIENT_ACKNOWLEDGE},
   * {@link javax.jms.Session#SESSION_TRANSACTED}.
   */
  public AbstractSessionConsumerCreator(final PubSubConnection connection, final boolean transacted, final int acknowledgeMode)
  {
    super(connection, transacted, acknowledgeMode);
  }

  @Override
  public MessageConsumer createConsumer(final Destination destination) throws JMSException
  {
    return null;
  }

  @Override
  public MessageConsumer createConsumer(final Destination destination, final String messageSelector) throws JMSException
  {
    return null;
  }

  @Override
  public MessageConsumer createConsumer(final Destination destination, final String messageSelector, final boolean noLocal) throws JMSException
  {
    return null;
  }

  @Override
  public MessageConsumer createSharedConsumer(final Topic topic, final String sharedSubscriptionName) throws JMSException
  {
    return null;
  }

  @Override
  public MessageConsumer createSharedConsumer(final Topic topic,
                                              final String sharedSubscriptionName,
                                              final String messageSelector) throws JMSException
  {
    return null;
  }

  @Override
  public MessageConsumer createDurableConsumer(final Topic topic, final String name) throws JMSException
  {
    return null;
  }

  @Override
  public MessageConsumer createDurableConsumer(final Topic topic,
                                               final String name,
                                               final String messageSelector,
                                               final boolean noLocal) throws JMSException
  {
    return null;
  }

  @Override
  public MessageConsumer createSharedDurableConsumer(final Topic topic, final String name) throws JMSException
  {
    return null;
  }

  @Override
  public MessageConsumer createSharedDurableConsumer(final Topic topic, final String name, final String messageSelector) throws JMSException
  {
    return null;
  }
  
}
