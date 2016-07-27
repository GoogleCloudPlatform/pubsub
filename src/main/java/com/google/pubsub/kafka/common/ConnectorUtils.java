// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////
package com.google.pubsub.kafka.common;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.internal.ManagedChannelImpl;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.kafka.connect.errors.ConnectException;

/** Utility methods and constants that are repeated across one or more classes. */
public class ConnectorUtils {

  private static final String ENDPOINT = "pubsub.googleapis.com";
  private static final List<String> CPS_SCOPE =
      Arrays.asList("https://www.googleapis.com/auth/pubsub");

  public static final String SCHEMA_NAME = ByteString.class.getName();
  public static final String CPS_SUBSCRIPTION_FORMAT = "projects/%s/subscriptions/%s";
  public static final String CPS_TOPIC_FORMAT = "projects/%s/topics/%s";
  public static final String CPS_PROJECT_CONFIG = "cps.project";
  public static final String CPS_TOPIC_CONFIG = "cps.topic";

  /** Return {@link io.grpc.Channel} which is used by Cloud Pub/Sub gRPC API's. */
  public static Channel getChannel() throws IOException {
    ManagedChannelImpl channelImpl =
        NettyChannelBuilder.forAddress(ENDPOINT, 443).negotiationType(NegotiationType.TLS).build();
    final ClientAuthInterceptor interceptor =
        new ClientAuthInterceptor(
            GoogleCredentials.getApplicationDefault().createScoped(CPS_SCOPE),
            Executors.newCachedThreadPool());
    return ClientInterceptors.intercept(channelImpl, interceptor);
  }

  /** Validates whether a required configuration value exists and is valid. Returns the value. */
  public static String validateConfig(Map<String, String> props, String configKey) {
    String configValue = props.get(configKey);
    if (configValue == null || configValue.isEmpty()) {
      throw new ConnectException("Missing the required config: " + configKey);
    }
    return configValue;
  }
}
