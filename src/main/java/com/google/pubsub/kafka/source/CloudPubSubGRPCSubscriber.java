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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import java.io.IOException;

/**
 * A {@link CloudPubSubSubscriber} that uses <a href="http://www.grpc.io/">gRPC</a> to pull
 * messages from <a href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>.
 */
public class CloudPubSubGRPCSubscriber implements CloudPubSubSubscriber {

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

  public ListenableFuture<Empty> ackMessages(AcknowledgeRequest request) {
    return subscriber.acknowledge(request);
  }
}
