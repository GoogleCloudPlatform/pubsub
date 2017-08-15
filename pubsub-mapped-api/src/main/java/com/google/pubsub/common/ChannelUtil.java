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

  private final ManagedChannel channel;
  private final CallCredentials callCredentials;

  private ChannelUtil() throws IOException {
    this.callCredentials = MoreCallCredentials.from(GoogleCredentials.getApplicationDefault());
    this.channel = ManagedChannelBuilder.forTarget(ENDPOINT).build();
  }

  private static synchronized ChannelUtil buildInstance() {
    try {
      return new ChannelUtil();
    } catch (IOException e) {
      throw new RuntimeException("Exception while authorising with cloud", e);
    }
  }

  public static synchronized ChannelUtil getInstance() {
    return INSTANCE;
  }

  public synchronized Channel getChannel() {
    return channel;
  }

  public synchronized CallCredentials getCallCredentials() {
    return callCredentials;
  }
}
