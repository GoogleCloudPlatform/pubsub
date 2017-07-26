package com.google.pubsub.common;

import com.google.auth.oauth2.GoogleCredentials;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by pietrzykp on 7/24/17.
 */
public class ChannelUtil {
  private static final Logger log = LoggerFactory.getLogger(PubsubChannelUtil.class);
  private static final String ENDPOINT = "pubsub.googleapis.com";

  private ManagedChannel channel;
  private CallCredentials callCredentials;

  /* Constructs instance with populated credentials and channel */
  public ChannelUtil() {
    try {
      callCredentials = MoreCallCredentials.from(GoogleCredentials.getApplicationDefault());
      channel = ManagedChannelBuilder.forTarget(ENDPOINT).build();
    } catch (IOException e) {
      log.error("Exception while authorising with cloud: " + e.getMessage());
      return;
    }
  }

  public CallCredentials getCallCredentials() {
    return callCredentials;
  }

  public Channel getChannel() {
    return channel;
  }

  public void closeChannel() {
    channel.shutdown();
  }
}
