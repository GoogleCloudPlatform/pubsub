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
import io.grpc.ManagedChannel;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

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
  public static final String CPS_MESSAGE_KEY_ATTRIBUTE = "key";
  public static final String KAFKA_MESSAGE_CPS_BODY_FIELD = "message";
  public static final String KAFKA_TOPIC_ATTRIBUTE = "kafka.topic";
  public static final String KAFKA_PARTITION_ATTRIBUTE = "kafka.partition";
  public static final String KAFKA_OFFSET_ATTRIBUTE = "kafka.offset";
  public static final String KAFKA_TIMESTAMP_ATTRIBUTE = "kafka.timestamp";

  /** Return {@link io.grpc.Channel} which is used by Cloud Pub/Sub gRPC API's. */
  public static Channel getChannel(String credKeyPath) throws IOException {
    ManagedChannel channelImpl =
        NettyChannelBuilder.forAddress(ENDPOINT, 443)
            .negotiationType(NegotiationType.TLS)
            // Maximum Pub/Sub message size is 10MB.
            .maxInboundMessageSize(10 * 1024 * 1024)
            .build();
    GoogleCredentials googleCredentials = credKeyPath == null ? 
        GoogleCredentials.getApplicationDefault() : GoogleCredentials.fromStream(new FileInputStream(credKeyPath));   
    final ClientAuthInterceptor interceptor =
        new ClientAuthInterceptor(
            googleCredentials.createScoped(CPS_SCOPE),
            Executors.newCachedThreadPool());
    return ClientInterceptors.intercept(channelImpl, interceptor);
  }
}
