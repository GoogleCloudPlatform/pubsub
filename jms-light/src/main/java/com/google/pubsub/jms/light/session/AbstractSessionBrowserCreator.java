package com.google.pubsub.jms.light.session;

import com.google.pubsub.jms.light.PubSubConnection;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;

/**
 * Default implementation of {@link javax.jms.Session} browser creations. 
 *
 * @author Maksym Prokhorenko
 */
public abstract class AbstractSessionBrowserCreator extends AbstractSessionConsumerCreator
{
  /**
   * Default constructor.
   * @param connection is a jms connection.
   * @param transacted is an indicator whether the session in transacted mode.
   * @param acknowledgeMode is an acknowledgement mode {@link javax.jms.Session#AUTO_ACKNOWLEDGE}, {@link javax.jms.Session#CLIENT_ACKNOWLEDGE},
   * {@link javax.jms.Session#SESSION_TRANSACTED}.
   */
  public AbstractSessionBrowserCreator(final PubSubConnection connection, final boolean transacted, final int acknowledgeMode)
  {
    super(connection, transacted, acknowledgeMode);
  }

  @Override
  public QueueBrowser createBrowser(final Queue queue) throws JMSException
  {
    return null;
  }

  @Override
  public QueueBrowser createBrowser(final Queue queue, final String messageSelector) throws JMSException
  {
    return null;
  }
}
