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
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rramkumar on 7/14/16.
 */
public class CloudPubSubRoundRobinSubscriber implements CloudPubSubSubscriber{

  private List<CloudPubSubSubscriber> subscribers;
  private int currentSubscriberIndex = 0;

  public CloudPubSubRoundRobinSubscriber(int subscriberCount) {
    subscribers = new ArrayList<>();
    for (int i = 0; i < subscriberCount; ++i) {
      subscribers.add(new CloudPubSubGRPCSubscriber());
    }
  }

  @Override
  public ListenableFuture<PullResponse> pull(PullRequest request) {
    currentSubscriberIndex = (currentSubscriberIndex+ 1) % subscribers.size();
    return subscribers.get(currentSubscriberIndex).pull(request);
  }

  @Override
  public ListenableFuture<Empty> ackMessages(AcknowledgeRequest request) {
    currentSubscriberIndex = (currentSubscriberIndex + 1) % subscribers.size();
    return subscribers.get(currentSubscriberIndex).ackMessages(request);
  }
}
