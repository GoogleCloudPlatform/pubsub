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
package com.google.pubsub.kafka.sink;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherFutureStub;
import java.io.IOException;

/**
 * * A {@link CloudPubSubPublisher} that uses <a href="http://www.grpc.io/">gRPC</a> to send
 * messages to <a href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>.
 */
public class CloudPubSubGRPCPublisher implements CloudPubSubPublisher {

  private PublisherFutureStub publisher;

  public CloudPubSubGRPCPublisher() {
    try {
      publisher = PublisherGrpc.newFutureStub(ConnectorUtils.getChannel());
    } catch (IOException e) {
      throw new RuntimeException("Could not create publisher stub; no publishes can occur.");
    }
  }

  @Override
  public ListenableFuture<PublishResponse> publish(PublishRequest request) {
    return publisher.publish(request);
  }
}
