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

package com.google.pubsub.clients.experimental;

import com.beust.jcommander.JCommander;
import com.google.cloud.pubsub.Subscriber;
import com.google.cloud.pubsub.Subscriber.MessageReceiver.AckReply;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.clients.common.Task.RunResult;
import com.google.pubsub.flic.common.LoadtestProto.StartRequest;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs a task that subscribes to a Cloud Pub/Sub topic. */
class CPSSubscriberTask extends Task implements Subscriber.MessageReceiver {
  private static final Logger log = LoggerFactory.getLogger(CPSSubscriberTask.class);
  private final Subscriber subscriber;

  private CPSSubscriberTask(StartRequest request) {
    super(request, "experimental", MetricsHandler.MetricName.END_TO_END_LATENCY);
    this.subscriber =
        Subscriber.Builder.newBuilder(
                "projects/"
                    + request.getProject()
                    + "/subscriptions/"
                    + request.getSubscription(),
                this)
            .build();
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options options = new LoadTestRunner.Options();
    new JCommander(options, args);
    LoadTestRunner.run(options, CPSSubscriberTask::new);
  }

  @Override
  public ListenableFuture<AckReply> receiveMessage(PubsubMessage message) {
    recordMessageLatency(
        Integer.parseInt(message.getAttributesMap().get("clientId")),
        Integer.parseInt(message.getAttributesMap().get("sequenceNumber")),
        System.currentTimeMillis() - Long.parseLong(message.getAttributesMap().get("sendTime")));
    return Futures.immediateFuture(AckReply.ACK);
  }

  @Override
  public ListenableFuture<RunResult> doRun() {
    synchronized (subscriber) {
      if (subscriber.isRunning()) {
        return Futures.immediateFuture(RunResult.empty());
      }
      try {
        subscriber.startAsync().awaitRunning();
      } catch (Exception e) {
        log.error("Fatal error from subscriber.", e);
        return Futures.immediateFailedFuture(e);
      }
      return Futures.immediateFuture(RunResult.empty());
    }
  }

  @Override
  public void shutdown() {
    synchronized (subscriber) {
      subscriber.stopAsync().awaitTerminated();
    }
  }
}
