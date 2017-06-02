package com.google.pubsub.jms.light;

import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 * Default PubSub {@link MessageProducer} implementation.
 *
 * @author Maksym Prokhorenko
 */
public abstract class AbstractMessageProducer implements MessageProducer {
  private final Destination destination;
  private final Session session;

  private boolean closed;
  private boolean disableMessageId;
  private boolean disableTimestamp;
  private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
  private int priority = Message.DEFAULT_PRIORITY;
  private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
  private long deliveryDelay = Message.DEFAULT_DELIVERY_DELAY;

  /**
   * Default constructor.
   * @param session is a jms session.
   * @param destination is a jms destination.
   */
  public AbstractMessageProducer(final Session session, final Destination destination) {
    this.destination = destination;
    this.session = session;
  }

  protected Session getSession() {
    return session;
  }

  @Override
  public void setDisableMessageID(final boolean disableMessageId) throws JMSException {
    this.disableMessageId = disableMessageId;
  }

  @Override
  public boolean getDisableMessageID() throws JMSException {
    return disableMessageId;
  }

  @Override
  public void setDisableMessageTimestamp(final boolean disableTimestamp) throws JMSException {
    this.disableTimestamp = disableTimestamp;
  }

  @Override
  public boolean getDisableMessageTimestamp() throws JMSException {
    return disableTimestamp;
  }

  @Override
  public void setDeliveryMode(final int deliveryMode) throws JMSException {
    this.deliveryMode = deliveryMode;
  }

  @Override
  public int getDeliveryMode() throws JMSException {
    return deliveryMode;
  }

  @Override
  public void setPriority(final int defaultPriority) throws JMSException {
    this.priority = defaultPriority;
  }

  @Override
  public int getPriority() throws JMSException {
    return priority;
  }

  @Override
  public void setTimeToLive(final long timeToLive) throws JMSException {
    this.timeToLive = timeToLive;
  }

  @Override
  public long getTimeToLive() throws JMSException {
    return timeToLive;
  }

  @Override
  public void setDeliveryDelay(final long deliveryDelay) throws JMSException {
    this.deliveryDelay = deliveryDelay;
  }

  @Override
  public long getDeliveryDelay() throws JMSException {
    return deliveryDelay;
  }

  @Override
  public Destination getDestination() throws JMSException {
    return destination;
  }

  @Override
  public void close() throws JMSException {
    closed = true;
  }

  protected boolean isClosed() {
    return closed;
  }

  @Override
  public void send(final Message message) throws JMSException {
    send(destination, message, deliveryMode, priority, timeToLive, null);
  }

  @Override
  public void send(final Message message,
                   final int deliveryMode,
                   final int priority,
                   final long timeToLive) throws JMSException {
    send(destination, message, deliveryMode, priority, timeToLive, null);
  }

  @Override
  public void send(final Destination destination,
                   final Message message) throws JMSException {
    send(destination, message, deliveryMode, priority, timeToLive, null);
  }

  @Override
  public void send(final Destination destination,
                   final Message message,
                   final int deliveryMode,
                   final int priority,
                   final long timeToLive) throws JMSException {
    send(destination, message, deliveryMode, priority, timeToLive, null);
  }

  @Override
  public void send(final Message message,
                   final CompletionListener completionListener) throws JMSException {
    send(getDestination(),
        message, deliveryMode, priority, timeToLive, completionListener);
  }

  @Override
  public void send(final Message message,
                   final int deliveryMode,
                   final int priority,
                   final long timeToLive,
                   final CompletionListener completionListener) throws JMSException {
    send(destination, message, deliveryMode, priority, timeToLive, completionListener);
  }

  @Override
  public void send(final Destination destination,
                   final Message message,
                   final CompletionListener completionListener) throws JMSException {
    send(destination, message, deliveryMode, priority, timeToLive, completionListener);
  }
}
