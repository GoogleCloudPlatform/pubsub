package com.google.pubsub.jms.light.session;

import com.google.common.collect.Sets;
import com.google.pubsub.jms.light.PubSubConnection;
import com.google.pubsub.jms.light.PubSubMessageProducer;

import java.util.Set;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;

/**
 * Default implementation of {@link javax.jms.Session} producer creations. 
 *
 * @author Maksym Prokhorenko
 */
public abstract class AbstractSessionProducerCreator extends AbstractSessionQueueCreator {
  private final Set<MessageProducer> producers = Sets.newConcurrentHashSet();

  /**
   * Default constructor.
   * @param connection is a jms connection.
   * @param transacted is an indicator whether the session in transacted mode.
   * @param acknowledgeMode is an acknowledgement mode {@link javax.jms.Session#AUTO_ACKNOWLEDGE},
   *        {@link javax.jms.Session#CLIENT_ACKNOWLEDGE},
   *        {@link javax.jms.Session#SESSION_TRANSACTED}.
   */
  public AbstractSessionProducerCreator(
      final PubSubConnection connection,
      final boolean transacted,
      final int acknowledgeMode) {
    super(connection, transacted, acknowledgeMode);
  }

  @Override
  public MessageProducer createProducer(final Destination destination) throws JMSException {
    return new PubSubMessageProducer(this, destination);
  }

  protected void closeProducers() throws JMSException {
    for (final MessageProducer producer : producers) {
      producer.close();
    }
  }
}
