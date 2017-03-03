package com.google.pubsub.jms.light;

import com.google.pubsub.jms.light.session.AbstractSessionBrowserCreator;

import javax.jms.JMSException;

/**
 * Default PubSub {@link javax.jms.Session} implementation.
 *
 * @author Maksym Prokhorenko
 */
public class PubSubSession extends AbstractSessionBrowserCreator
{

  /**
   * Default constructor.
   * @param connection is a jms connection.
   * @param transacted is an indicator whether the session in transacted mode.
   * @param acknowledgeMode is an acknowledgement mode {@link javax.jms.Session#AUTO_ACKNOWLEDGE}, {@link javax.jms.Session#CLIENT_ACKNOWLEDGE},
   * {@link javax.jms.Session#SESSION_TRANSACTED}.
   */
  public PubSubSession(final PubSubConnection connection, final boolean transacted, final int acknowledgeMode)
  {
    super(connection, transacted, acknowledgeMode);
  }

  @Override
  public void commit() throws JMSException
  {
  }

  @Override
  public void rollback() throws JMSException
  {
  }

  @Override
  public void close() throws JMSException
  {
    closeProducers();
  }

  @Override
  public void recover() throws JMSException
  {
  }

  @Override
  public void run()
  {
  }

  @Override
  public void unsubscribe(final String name) throws JMSException
  {
  }
}
