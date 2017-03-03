package com.google.pubsub.jms.light;

import com.google.api.gax.core.RpcFuture;
import com.google.api.gax.core.RpcFutureCallback;
import com.google.cloud.pubsub.spi.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.Topic;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default PubSub {@link javax.jms.MessageProducer} implementation.
 *
 * @author Maksym Prokhorenko
 */
public class PubSubMessageProducer extends AbstractMessageProducer
{
  private static final Logger LOGGER = Logger.getLogger(PubSubMessageProducer.class.getName());
  private Publisher publisher;

  /**
   * Default PubSub's message producer constructor.
   * @param session is a jms session.
   * @param destination is a jms destination.
   * @throws JMSException in case {@link PubSubMessageProducer} doesn't support destination
   *   or destination object fails to return topic/queue name.
   */
  public PubSubMessageProducer(final Session session,
                               final Destination destination)
      throws JMSException
  {
    super(session, destination);
    publisher = createPublisher(destination);
  }

  @Override
  public void send(final Destination destination,
                   final Message message,
                   final int deliveryMode,
                   final int priority,
                   final long timeToLive,
                   final CompletionListener completionListener) throws JMSException
  {
    if (isClosed())
    {
      throw new IllegalStateException("Producer has been closed.");
    }

    if (!getDestination().equals(destination))
    {
      throw new IllegalArgumentException("Destination [" + destination
          + "] is invalid. Expected [" + getDestination() + "].");
    }

    final RpcFuture<String> messageIdFuture = publisher.publish(
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(message.getBody(String.class)))
            .build());

    messageIdFuture.addCallback(
        new RpcFutureCallback<String>()
        {
          @Override public void onSuccess(final String messageId)
          {
            LOGGER.fine(String.format("%s has been sent successfully.", messageId));
            if (null != completionListener)
            {
              completionListener.onCompletion(message);
            }
          }

          @Override public void onFailure(final Throwable thrown)
          {
            LOGGER.log(Level.SEVERE, "Message sending error:", thrown);
            if (null != completionListener)
            {
              completionListener.onException(message, (Exception) thrown);
            }
          }
        });
  }

  protected Publisher createPublisher(final Destination destination) throws JMSException
  {
    final Publisher result;

    if (destination instanceof Topic)
    {
      result = createPublisher((Topic) destination);
    }
    else
    {
      throw new InvalidDestinationException("Unsupported destination.");
    }

    return result;
  }

  protected Publisher createPublisher(final Topic topic) throws JMSException
  {
    final PubSubConnection connection = ((PubSubSession) getSession()).getConnection();
    try
    {
      return Publisher
          .newBuilder(TopicName.parse(topic.getTopicName()))
          .setChannelProvider(connection.getProviderManager())
          .setExecutorProvider(connection.getProviderManager())
          .setFlowControlSettings(connection.getFlowControlSettings())
          .setRetrySettings(connection.getRetrySettings())
          .build();
    }
    catch (final IOException e)
    {
      LOGGER.log(Level.SEVERE, "Can't create publisher.", e);
      throw new JMSException(e.getMessage());
    }
  }

  @SuppressWarnings("PMD.AvoidCatchingGenericException")
  @Override
  public synchronized void close() throws JMSException
  {
    super.close();

    try
    {
      publisher.shutdown();
    }
    catch (final Exception e)
    {
      LOGGER.log(Level.SEVERE, "Can't close message producer.", e);
      throw new JMSException(e.getMessage());
    }
  }
}
