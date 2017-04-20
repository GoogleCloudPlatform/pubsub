package com.google.pubsub.jms.light;

import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSubscriber;
import javax.jms.TemporaryTopic;

/**
 * Default PubSub {@link javax.jms.TopicSession} implementation.
 *
 * @author Daiqian Zhang
 */
class PubSubTopicSession extends PubSubSession {

  /**
   * Default constructor.
   * @param connection is a jms connection.
   * @param transacted is an indicator whether the session in transacted mode.
   * @param acknowledgeMode is an acknowledgement mode {@link javax.jms.Session#AUTO_ACKNOWLEDGE},
   *        {@link javax.jms.Session#CLIENT_ACKNOWLEDGE},
   *        {@link javax.jms.Session#SESSION_TRANSACTED}.
   */
  public PubSubTopicSession(
      final PubSubConnection connection,
      final boolean transacted,
      final int acknowledgeMode) {
    super(connection, transacted, acknowledgeMode);
  }

  @Override
  public TemporaryTopic createTemporaryTopic() {
    return null;
  }

  @Override
  public void unsubscribe(String name) {
  }

  // @Override
  // public QueueBrowser createBrowser(final Queue queue) throws
  // public createQueue
  // public createTemporaryQueue
}
