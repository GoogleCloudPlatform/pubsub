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
package com.google.pubsub.flic.cps;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.flic.common.Utils;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherFutureStub;
import java.util.ArrayList;
import java.util.List;

/** Publishes to Cloud Pub/Sub using a round robin scheme. */
public class CPSRoundRobinPublisher {

  private List<PublisherFutureStub> clients;
  private int currentClientIdx = 0;

  public CPSRoundRobinPublisher(int numClients, String cpsApi) throws Exception {
    clients = new ArrayList<>(numClients);
    for (int i = 0; i < numClients; ++i) {
      // Each stub gets its own channel
      clients.add(PublisherGrpc.newFutureStub(Utils.createChannel(cpsApi)));
    }
  }

  /** Allows each PublishRequest to get distributed to different clients. */
  public ListenableFuture<PublishResponse> publish(PublishRequest request) {
    currentClientIdx = (currentClientIdx + 1) % clients.size();
    return clients.get(currentClientIdx).publish(request);
  }
}
