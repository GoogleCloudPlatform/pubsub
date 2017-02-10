package com.google.pubsub.jms.light;

import com.google.cloud.pubsub.Publisher;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.Topic;

/**
 * Default PubSub {@link javax.jms.MessageProducer} implementation.
 *
 * @author Maksym Prokhorenko
 */
public class PubSubMessageProducer extends AbstractMessageProducer
{
  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubMessageProducer.class);
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
      throw new IllegalArgumentException("Destination [" + destination + "] is invalid. Expected ["
          + getDestination() + "].");
    }

    Futures.addCallback(publisher.publish(
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(message.getBody(String.class)))
            .build()),
        new FutureCallback<String>()
        {
          @Override
          public void onSuccess(final String messageId)
          {
            if (null != completionListener)
            {
              completionListener.onCompletion(message);
            }
          }

          @Override
          public void onFailure(final Throwable thrown)
          {
            LOGGER.error("Message sending error:", thrown);
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

  protected Publisher createPublisher(final Topic topic)
      throws JMSException
  {
    final PubSubConnection connection = ((PubSubSession) getSession()).getConnection();
    return Publisher.Builder.newBuilder(topic.getTopicName())
        .setChannelBuilder(connection.getChannelBuilder())
        .setCredentials(connection.getCredentials())
        .build();
  }

  @Override
  public synchronized void close() throws JMSException
  {
    super.close();
    publisher.shutdown();
  }
}
