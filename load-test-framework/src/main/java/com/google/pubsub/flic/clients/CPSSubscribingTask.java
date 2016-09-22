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
package com.google.pubsub.flic.clients;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.flic.common.Utils;
import com.google.pubsub.flic.cps.CPSRoundRobinSubscriber;
import com.google.pubsub.flic.processing.MessageProcessingHandler;
import com.google.pubsub.flic.task.CPSTask;
import com.google.pubsub.flic.task.TaskArgs;
import com.google.pubsub.v1.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Runs a subscribing task that consumes messages from Cloud Pub/Sub using gRPC.
 */
public class CPSSubscribingTask extends CPSTask {
  private final String subscription;
  private final int maxMessagesPerPull;
  private final int maxOpenPullsPerSubscription;
  private final Semaphore semaphore;

  private CPSRoundRobinSubscriber subscriber;
  private MessageProcessingHandler processingHandler;

  public CPSSubscribingTask(
      TaskArgs args, CPSRoundRobinSubscriber subscriber, MessageProcessingHandler processingHandler) {
    super(args);
    this.subscriber = subscriber;
    this.processingHandler = processingHandler;
    this.maxMessagesPerPull = args.getMaxMessagesPerPull();
    this.maxOpenPullsPerSubscription = args.getNumResponseThreads();
    this.semaphore = new Semaphore(maxOpenPullsPerSubscription);
    this.subscription = args.getSubscription();
  }

  /**
   * Consumes messages from Cloud Pub/Sub in the most optimal way possible
   */
  public void execute() {
    // Get the overall start time.
    long start = System.currentTimeMillis();
    while (!failureFlag.get()) {
      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        failureFlag.set(true);
      }
      PullRequest request = PullRequest.newBuilder()
          .setSubscription(this.subscription)
          .setReturnImmediately(true)
          .setMaxMessages(maxMessagesPerPull)
          .build();
      ListenableFuture<PullResponse> response = subscriber.pull(request);
      Futures.addCallback(
          response,
          new FutureCallback<PullResponse>() {
            @Override
            public void onSuccess(PullResponse response) {
              List<String> ackIds = new ArrayList<String>();
              long receivedTime = System.currentTimeMillis();
              for (ReceivedMessage recvMsg : response.getReceivedMessagesList()) {
                PubsubMessage message = recvMsg.getMessage();
                Map<String, String> attributes = message.getAttributes();
                ackIds.add(recvMsg.getAckId());
                // TODO(maxdietz): Replace this with streamz exporting
                if (attributes.get(Utils.TIMESTAMP_ATTRIBUTE) != null) {
                  // Calculate end-to-end latency if this message has the appropriate attribute.
                  long latency =
                      receivedTime - Long.valueOf(attributes.get(Utils.TIMESTAMP_ATTRIBUTE));
                  processingHandler.addStats(1, latency, message.getData().size());
                } else {
                  // Add a dummy value if we cannot calculate end-to-end latency.
                  processingHandler.addStats(1, 0, message.getData().size());
                }
              }
              subscriber.acknowledge(AcknowledgeRequest.newBuilder()
                  .setSubscription(subscription)
                  .addAllAckIds(ackIds)
                  .build());
              semaphore.release();
            }

            @Override
            public void onFailure(Throwable t) {
              if (!failureFlag.get()) {
                failureFlag.set(true);
              }
            }
          },
          callbackExecutor);
    }
    callbackExecutor.shutdownNow();
  }
}
