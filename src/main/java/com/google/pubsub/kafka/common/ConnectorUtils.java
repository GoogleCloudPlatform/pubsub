package com.google.pubsub.kafka.common;

import com.google.auth.oauth2.GoogleCredentials;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.internal.ManagedChannelImpl;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Created by rramkumar on 6/29/16.
 */
public class ConnectorUtils {

  private static final String ENDPOINT = "pubsub-experimental.googleapis.com";
  private static final List<String> CPS_SCOPE = Arrays.asList("https://www.googleapis.com/auth/pubsub");

  private ConnectorUtils() {}

  public static Channel getChannel() throws IOException{
    final ManagedChannelImpl channelImpl =
        NettyChannelBuilder.forAddress(ENDPOINT, 443).negotiationType(NegotiationType.TLS).build();
    try {
      final ClientAuthInterceptor interceptor =
          new ClientAuthInterceptor(
              GoogleCredentials.getApplicationDefault().createScoped(CPS_SCOPE),
              Executors.newCachedThreadPool());
      return ClientInterceptors.intercept(channelImpl, interceptor);
    } catch (IOException e) {
      throw e;
    }
  }
}
