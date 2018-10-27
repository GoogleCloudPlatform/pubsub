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
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link CloudPubSubSubscriber} that uses <a href="http://www.grpc.io/">gRPC</a> to pull messages
 * from <a href="https://cloud.google.com/pubsub">Google Cloud Pub/Sub</a>. This class is not
 * thread-safe.
 */
public class CloudPubSubGRPCSubscriber implements CloudPubSubSubscriber {

  private static final Logger log = LoggerFactory.getLogger(CloudPubSubGRPCSubscriber.class);
  private long nextSubscriberResetTime = 0;
  private SubscriberFutureStub subscriber;
  private Random rand = new Random(System.currentTimeMillis());
  private String gcpCredentialsFilePath;

  CloudPubSubGRPCSubscriber(String gcpCredentialsFilePath) {
    makeSubscriber();
    this.gcpCredentialsFilePath = gcpCredentialsFilePath;
  }

  public ListenableFuture<PullResponse> pull(PullRequest request) {
    if (System.currentTimeMillis() > nextSubscriberResetTime) {
      makeSubscriber();
    }
    return subscriber.pull(request);
  }

  public ListenableFuture<Empty> ackMessages(AcknowledgeRequest request) {
    if (System.currentTimeMillis() > nextSubscriberResetTime) {
      makeSubscriber();
    }
    return subscriber.acknowledge(request);
  }

  private void makeSubscriber() {
    try {
      log.info("Creating subscriber.");
      subscriber = SubscriberGrpc.newFutureStub(ConnectorUtils.getChannel(gcpCredentialsFilePath));
      // We change the subscriber every 25 - 35 minutes in order to avoid GOAWAY errors.
      nextSubscriberResetTime =
          System.currentTimeMillis() + rand.nextInt(10 * 60 * 1000) + 25 * 60 * 1000;
    } catch (IOException e) {
      throw new RuntimeException("Could not create subscriber stub; no subscribing can occur.", e);
    }
  }
}
