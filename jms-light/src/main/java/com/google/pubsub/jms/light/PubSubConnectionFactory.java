package com.google.pubsub.jms.light;

import com.google.api.gax.core.FlowControlSettings;
import com.google.api.gax.core.RetrySettings;
import com.google.api.gax.grpc.ProviderManager;
import com.google.common.base.MoreObjects;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import org.joda.time.Duration;

/**
 * Default PubSub {@link ConnectionFactory} implementation.
 *
 * @author Maksym Prokhorenko
 */
public class PubSubConnectionFactory implements ConnectionFactory {
  private ProviderManager providerManager;
  private FlowControlSettings flowControlSettings;
  private RetrySettings retrySettings;

  @Override
  public Connection createConnection() throws JMSException {
    return new PubSubConnection(providerManager,
        MoreObjects.firstNonNull(flowControlSettings, getDefaultFlowControllerSettings()),
        MoreObjects.firstNonNull(retrySettings, getDefaultRetrySettings()));
  }

  @Override
  public Connection createConnection(
      final String userName,
      final String password) throws JMSException {
    return null;
  }

  @Override
  public JMSContext createContext() {
    return null;
  }

  @Override
  public JMSContext createContext(final String userName, final String password) {
    return null;
  }

  @Override
  public JMSContext createContext(
      final String userName,
      final String password,
      final int sessionMode) {
    return null;
  }

  @Override
  
  public JMSContext createContext(final int sessionMode) {
    return null;
  }

  public void setProviderManager(final ProviderManager providerManager) {
    this.providerManager = providerManager;
  }

  public void setFlowControlSettings(final FlowControlSettings flowControlSettings) {
    this.flowControlSettings = flowControlSettings;
  }

  public void setRetrySettings(final RetrySettings retrySettings) {
    this.retrySettings = retrySettings;
  }

  @SuppressWarnings("checkstyle:magicnumber")
  protected RetrySettings getDefaultRetrySettings() {
    return RetrySettings.newBuilder()
        .setTotalTimeout(Duration.standardSeconds(15L))
        .setInitialRetryDelay(Duration.millis(200))
        .setRetryDelayMultiplier(2d)
        .setMaxRetryDelay(Duration.standardSeconds(5L))
        .setInitialRpcTimeout(Duration.millis(200))
        .setRpcTimeoutMultiplier(2d)
        .setMaxRpcTimeout(Duration.standardSeconds(5L))
        .build();
  }

  protected FlowControlSettings getDefaultFlowControllerSettings() {
    return FlowControlSettings.getDefaultInstance();
  }
}
