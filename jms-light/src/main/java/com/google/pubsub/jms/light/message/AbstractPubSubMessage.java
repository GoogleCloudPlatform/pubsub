package com.google.pubsub.jms.light.message;

import com.google.common.base.Charsets;

import java.util.logging.Logger;
import javax.jms.Destination;
import javax.jms.JMSException;

/**
 * Default PubSub {@link javax.jms.Message} implementation.
 *
 * @author Maksym Prokhorenko
 */
public abstract class AbstractPubSubMessage extends AbstractPropertyMessage {
  private static final Logger LOGGER = Logger.getLogger(AbstractPubSubMessage.class.getName());

  private boolean acknowledged;
  private String jmsMessageId;
  private long jmsTimestamp;
  private String correlationId;
  private Destination replyTo;
  private Destination destination;
  private int jmsDeliveryMode;
  private boolean jmsRedelivered;
  private String jmsType;
  private long jmsExpiration;
  private long jmsDeliveryTime;
  private int jmsPriority;

  @Override
  public void acknowledge() throws JMSException {
    if (acknowledged) {
      //TODO: seems as exceptional state.
      LOGGER.warning("Message already acknowledged. [" + getJMSMessageID() + "]");
    } else {
      acknowledged = true;
    }
  }

  @Override
  public String getJMSMessageID() throws JMSException {
    return jmsMessageId;
  }

  @Override
  public void setJMSMessageID(final String id) throws JMSException {
    this.jmsMessageId = id;
  }

  @Override
  public long getJMSTimestamp() throws JMSException {
    return jmsTimestamp;
  }

  @Override
  public void setJMSTimestamp(final long timestamp) throws JMSException {
    this.jmsTimestamp = timestamp;
  }

  @Override
  public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
    return null == correlationId ? null : correlationId.getBytes(Charsets.UTF_8);
  }

  @Override
  public void setJMSCorrelationIDAsBytes(final byte[] correlationId) throws JMSException {
    this.correlationId = null == correlationId ? null : new String(correlationId, Charsets.UTF_8);
  }

  @Override
  public void setJMSCorrelationID(final String correlationId) throws JMSException {
    this.correlationId = correlationId; 
  }

  @Override
  public String getJMSCorrelationID() throws JMSException {
    return correlationId;
  }

  @Override
  public Destination getJMSReplyTo() throws JMSException {
    return replyTo;
  }

  @Override
  public void setJMSReplyTo(final Destination replyTo) throws JMSException {
    this.replyTo = replyTo;
  }

  @Override
  public Destination getJMSDestination() throws JMSException {
    return destination;
  }

  @Override
  public void setJMSDestination(final Destination destination) throws JMSException {
    this.destination = destination;
  }

  @Override
  public int getJMSDeliveryMode() throws JMSException {
    return jmsDeliveryMode;
  }

  @Override
  public void setJMSDeliveryMode(final int deliveryMode) throws JMSException {
    this.jmsDeliveryMode = deliveryMode;
  }

  @Override
  public boolean getJMSRedelivered() throws JMSException {
    return jmsRedelivered;
  }

  @Override
  public void setJMSRedelivered(final boolean redelivered) throws JMSException {
    this.jmsRedelivered = redelivered;
  }

  @Override
  public String getJMSType() throws JMSException {
    return jmsType;
  }

  @Override
  public void setJMSType(final String type) throws JMSException {
    this.jmsType = type;
  }

  @Override
  public long getJMSExpiration() throws JMSException {
    return jmsExpiration;
  }

  @Override
  public void setJMSExpiration(final long expiration) throws JMSException {
    this.jmsExpiration = expiration;
  }

  @Override
  public long getJMSDeliveryTime() throws JMSException {
    return jmsDeliveryTime;
  }

  @Override
  public void setJMSDeliveryTime(final long deliveryTime) throws JMSException {
    this.jmsDeliveryTime = deliveryTime;
  }

  @Override
  public int getJMSPriority() throws JMSException {
    return jmsPriority;
  }

  @Override
  public void setJMSPriority(final int priority) throws JMSException {
    this.jmsPriority = priority;
  }
}
