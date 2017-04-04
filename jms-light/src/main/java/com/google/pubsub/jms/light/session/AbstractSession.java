package com.google.pubsub.jms.light.session;

import com.google.pubsub.jms.light.PubSubConnection;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;

/**
 * Basic {@link javax.jms.Session} argument storage.
 *
 * @author Maksym Prokhorenko
 */
public abstract class AbstractSession implements Session {
  private final PubSubConnection connection;
  private final boolean transacted;
  private final int acknowledgeMode;
  private MessageListener messageListener;

  /**
   * Default constructor.
   * @param connection is a jms connection.
   * @param transacted is an indicator whether the session in transacted mode.
   * @param acknowledgeMode is an acknowledgement mode {@link javax.jms.Session#AUTO_ACKNOWLEDGE},
   *        {@link javax.jms.Session#CLIENT_ACKNOWLEDGE},
   *        {@link javax.jms.Session#SESSION_TRANSACTED}.
   */
  public AbstractSession(
      final PubSubConnection connection,
      final boolean transacted,
      final int acknowledgeMode) {
    this.connection = connection;
    this.transacted = transacted;
    this.acknowledgeMode = acknowledgeMode;
  }

  public PubSubConnection getConnection() {
    return connection;
  }

  @Override
  public boolean getTransacted() throws JMSException {
    return transacted;
  }

  @Override
  public int getAcknowledgeMode() throws JMSException {
    return acknowledgeMode;
  }

  @Override
  public MessageListener getMessageListener() throws JMSException {
    return messageListener;
  }

  @Override
  public void setMessageListener(final MessageListener listener) throws JMSException {
    this.messageListener = listener;
  }
}
