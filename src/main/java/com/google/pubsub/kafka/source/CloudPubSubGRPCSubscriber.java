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
package com.google.pubsub.kafka.source;

import java.io.IOException;
import java.util.List;
import java.util.Arrays;
import java.util.concurrent.Executors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.util.concurrent.ListenableFuture;

import com.google.protobuf.Empty;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.kafka.sink.CloudPubSubGRPCPublisher;
import com.google.pubsub.v1.*;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.internal.ManagedChannelImpl;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudPubSubGRPCSubscriber implements CloudPubSubSubscriber {
  private static final Logger log = LoggerFactory.getLogger(CloudPubSubGRPCPublisher.class);

  private SubscriberFutureStub subscriber;

  CloudPubSubGRPCSubscriber() {
    try {
      subscriber = SubscriberGrpc.newFutureStub(ConnectorUtils.getChannel());
    } catch (IOException e) {
      throw new RuntimeException("Could not create subscriber stub; no pulls can occur.");
    }
  }

  public ListenableFuture<PullResponse> pull(PullRequest request) {
    return subscriber.pull(request);
  }

  public ListenableFuture<Subscription> createSubscription(Subscription request) {
    return subscriber.createSubscription(request);
  }

  public ListenableFuture<Empty> ackMessages(AcknowledgeRequest request) {
    return subscriber.acknowledge(request);
  }
}
