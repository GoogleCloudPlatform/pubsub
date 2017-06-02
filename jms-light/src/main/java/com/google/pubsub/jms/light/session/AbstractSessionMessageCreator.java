package com.google.pubsub.jms.light.session;

import com.google.pubsub.jms.light.PubSubConnection;
import com.google.pubsub.jms.light.message.PubSubTextMessage;

import java.io.Serializable;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

/**
 * Default implementation of {@link javax.jms.Session} message creations. 
 *
 * @author Maksym Prokhorenko
 */
public abstract class AbstractSessionMessageCreator extends AbstractSession {

  /**
   * Default constructor.
   * @param connection is a jms connection.
   * @param transacted is an indicator whether the session in transacted mode.
   * @param acknowledgeMode is an acknowledgement mode {@link javax.jms.Session#AUTO_ACKNOWLEDGE},
   *        {@link javax.jms.Session#CLIENT_ACKNOWLEDGE},
   *        {@link javax.jms.Session#SESSION_TRANSACTED}.
   */
  public AbstractSessionMessageCreator(
      final PubSubConnection connection,
      final boolean transacted,
      final int acknowledgeMode) {
    super(connection, transacted, acknowledgeMode);
  }

  @Override
  public BytesMessage createBytesMessage() throws JMSException {
    return null;
  }

  @Override
  public MapMessage createMapMessage() throws JMSException {
    return null;
  }

  @Override
  public Message createMessage() throws JMSException {
    return null;
  }

  @Override
  public ObjectMessage createObjectMessage() throws JMSException {
    return null;
  }

  @Override
  public ObjectMessage createObjectMessage(final Serializable object) throws JMSException {
    return null;
  }

  @Override
  public StreamMessage createStreamMessage() throws JMSException {
    return null;
  }

  @Override
  public TextMessage createTextMessage() throws JMSException {
    return new PubSubTextMessage();
  }

  @Override
  public TextMessage createTextMessage(final String text) throws JMSException {
    final TextMessage message = createTextMessage();
    message.setText(text);

    return message;
  }
}
