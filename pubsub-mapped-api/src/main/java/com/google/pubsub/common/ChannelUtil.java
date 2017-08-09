package com.google.pubsub.common;

import com.google.auth.oauth2.GoogleCredentials;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.auth.MoreCallCredentials;
import java.io.IOException;

public final class ChannelUtil {
  private static final String ENDPOINT = "pubsub.googleapis.com";

  private static final ChannelUtil INSTANCE = buildInstance();

  private ManagedChannel channel;
  private CallCredentials callCredentials;

  private ChannelUtil(){}

  private static ChannelUtil buildInstance() {
    try {
      ChannelUtil channelUtil = new ChannelUtil();
      channelUtil.callCredentials = MoreCallCredentials.from(GoogleCredentials.getApplicationDefault());
      channelUtil.channel = ManagedChannelBuilder.forTarget(ENDPOINT).build();
      return channelUtil;
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
