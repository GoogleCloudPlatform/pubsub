package com.google.pubsub.common;

import com.google.auth.oauth2.GoogleCredentials;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.auth.MoreCallCredentials;
import java.io.IOException;

public class ChannelUtil {
  private static final String ENDPOINT = "pubsub.googleapis.com";

  private static ChannelUtil INSTANCE;

  private ManagedChannel channel;
  private CallCredentials callCredentials;

  private ChannelUtil(){}

  static {
    try {
      INSTANCE = new ChannelUtil();
      INSTANCE.callCredentials = MoreCallCredentials.from(GoogleCredentials.getApplicationDefault());
      INSTANCE.channel = ManagedChannelBuilder.forTarget(ENDPOINT).build();
    } catch (IOException e) {
      throw new RuntimeException("Exception while authorising with cloud: " + e.getMessage());
    }
  }

  public static ChannelUtil getInstance() {
    return INSTANCE;
  }

  public Channel getChannel() {
    return channel;
  }

  public CallCredentials getCallCredentials() {
    return callCredentials;
  }
}
