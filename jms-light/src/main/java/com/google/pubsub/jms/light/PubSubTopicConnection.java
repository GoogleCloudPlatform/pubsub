package com.google.pubsub.jms.light;

import com.google.api.gax.core.RetrySettings;
import com.google.api.gax.grpc.FlowControlSettings;
import com.google.api.gax.grpc.ProviderManager;

import javax.jms.ConnectionConsumer;
import javax.jms.JMSException;
import javax.jms.ServerSessionPool;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

/**
 * Default PubSub {@link TopicConnectionFactory} implementation.
 *
 * @author Daiqian Zhang
 */
public class PubSubTopicConnection extends PubSubConnection implements TopicConnection {

  /**
   * Default Connection constructor.
   * @param providerManager is a channel and executor container. Used by Publisher/Subscriber.
   * @param flowControlSettings is a flow control: such as max outstanding messages and maximum
   *        outstanding bytes.
   * @param retrySettings is a retry logic configuration.
   */
  public PubSubTopicConnection(
      final ProviderManager providerManager,
      final FlowControlSettings flowControlSettings,
      final RetrySettings retrySettings) {
    super(providerManager, flowControlSettings, retrySettings);
  }

  @Override
  public ConnectionConsumer createConnectionConsumer(
      final Topic topic,
      final String messageSelector,
      final ServerSessionPool sessionPool,
      final int maxMessages) throws JMSException {
    return null;
  }

  @Override
  public ConnectionConsumer createDurableConnectionConsumer(
      final Topic topic,
      final String subscriptionName,
      final String messageSelector,
      final ServerSessionPool sessionPool,
      final int maxMessages) throws JMSException {
    return null;
  }

  @Override
  public TopicSession createTopicSession(
      final boolean transacted,
      final int acknowledgeMode) throws JMSException {
    return null;
  }
}
