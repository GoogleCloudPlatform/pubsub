package com.google.pubsub.clients.common;

import com.google.api.gax.grpc.LoadTestGrpcChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import java.io.IOException;

public class LoadTestSubscriptionAdminSettings extends SubscriptionAdminSettings {

  protected LoadTestSubscriptionAdminSettings(
      Builder settingsBuilder) throws IOException {
    super(settingsBuilder);
  }

  public static LoadTestGrpcChannelProvider.Builder defaultGrpcTransportBuilder() {
    return LoadTestGrpcChannelProvider.newBuilder();
  }
}
