package com.google.pubsub.clients.common;

import com.google.api.gax.grpc.LoadTestGrpcChannelProvider;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import java.io.IOException;

public class LoadTestTopicAdminSettings extends TopicAdminSettings {

  protected LoadTestTopicAdminSettings(Builder settingsBuilder) throws IOException {
    super(settingsBuilder);
  }

  public static LoadTestGrpcChannelProvider.Builder defaultGrpcTransportProvider() {
    return LoadTestGrpcChannelProvider.newBuilder();
  }
}
