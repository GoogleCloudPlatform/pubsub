package com.google.pubsub.jms.light;

import com.google.auth.oauth2.GoogleCredentials;
import io.grpc.ManagedChannelBuilder;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import java.io.IOException;

/**
 * Default PubSub {@link ConnectionFactory} implementation.
 *
 * @author Maksym Prokhorenko
 */
public class PubSubConnectionFactory implements ConnectionFactory
{

  private ManagedChannelBuilder channelBuilder;
  private GoogleCredentials credentials;

  @Override
  public Connection createConnection() throws JMSException
  {
    PubSubConnection result;
    try
    {
      result = new PubSubConnection(channelBuilder, credentials);
    }
    catch (final IOException e)
    {
      throw new JMSSecurityException("Credentials are broken or unavailable. " + e.getMessage());
    }

    return result;
  }

  @Override
  public Connection createConnection(final String userName, final String password) throws JMSException
  {
    return null;
  }

  @Override
  public JMSContext createContext()
  {
    return null;
  }

  @Override
  public JMSContext createContext(final String userName, final String password)
  {
    return null;
  }

  @Override
  public JMSContext createContext(final String userName, final String password, final int sessionMode)
  {
    return null;
  }

  @Override
  public JMSContext createContext(final int sessionMode)
  {
    return null;
  }

  public void setChannelBuilder(final ManagedChannelBuilder channelBuilder)
  {
    this.channelBuilder = channelBuilder;
  }

  public void setCredentials(final GoogleCredentials googleCredentials)
  {
    this.credentials = googleCredentials;
  }
}
