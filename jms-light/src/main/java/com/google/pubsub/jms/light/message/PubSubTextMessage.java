package com.google.pubsub.jms.light.message;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.TextMessage;

/**
 * Default PubSub {@link TextMessage} implementation.
 *
 * @author Maksym Prokhorenko
 */
public class PubSubTextMessage extends AbstractPubSubMessage implements TextMessage {
  private String payload;

  @Override
  public void setText(final String payload) throws JMSException {
    this.payload = payload;
  }

  @Override
  public String getText() throws JMSException {
    return payload;
  }

  @Override
  public void clearBody() throws JMSException {
    payload = null;
  }

  @Override
  public <T> T getBody(final Class<T> clazz) throws JMSException {
    final T result;
    if (isBodyAssignableTo(clazz)) {
      result = clazz.cast(getText());
    } else {
      throw new MessageFormatException("Can't be assigned to " + clazz);
    }

    return result; 
  }

  @Override
  public boolean isBodyAssignableTo(final Class clazz) throws JMSException {
    return clazz.equals(String.class);
  }
}
