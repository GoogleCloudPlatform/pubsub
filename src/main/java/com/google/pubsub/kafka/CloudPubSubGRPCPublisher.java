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
package com.google.pubsub.kafka;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.util.concurrent.ListenableFuture;

import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherFutureStub;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * A {@link CloudPubSubPublisher} that uses <a href="http://www.grpc.io/">gRPC</a> to send messages
 * to <a href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>.
 */
public class CloudPubSubGRPCPublisher implements CloudPubSubPublisher {
  private static final Logger log = LoggerFactory.getLogger(CloudPubSubGRPCPublisher.class);

  private static final String ENDPOINT = "pubsub-experimental.googleapis.com";
  private static final List<String> CPS_SCOPE =
      Arrays.asList("https://www.googleapis.com/auth/pubsub");

  private PublisherFutureStub publisher;

  public CloudPubSubGRPCPublisher() {
    final ManagedChannelImpl channelImpl =
        NettyChannelBuilder.forAddress(ENDPOINT, 443).negotiationType(NegotiationType.TLS).build();
    try {
      final ClientAuthInterceptor interceptor =
          new ClientAuthInterceptor(
              GoogleCredentials.getApplicationDefault().createScoped(CPS_SCOPE),
              Executors.newCachedThreadPool());
      final Channel channel = ClientInterceptors.intercept(channelImpl, interceptor);
      publisher = PublisherGrpc.newFutureStub(channel);
    } catch (IOException e) {
      log.error("Could not create publisher stub; no publishes can occur.");
    }
  }

  @Override
  public ListenableFuture<PublishResponse> publish(PublishRequest request) {
    return publisher.publish(request);
  }
}
