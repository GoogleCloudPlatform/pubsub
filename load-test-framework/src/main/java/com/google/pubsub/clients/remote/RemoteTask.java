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
package com.google.pubsub.clients.remote;

import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.flic.common.LoadtestGrpc;
import com.google.pubsub.flic.common.LoadtestProto;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;


/**
 * Runs a task that executes command on the remote server.
 */
class RemoteTask extends Task {
  private static final Logger log = LoggerFactory.getLogger(RemoteTask.class);
  private LoadtestGrpc.LoadtestBlockingStub stub;

  private RemoteTask(LoadtestProto.StartRequest request, MetricsHandler.MetricName metricName) {
    super(request.getProject(), "remote", metricName);
    stub = LoadtestGrpc.newBlockingStub(
        ManagedChannelBuilder.forAddress("localhost", 6000).usePlaintext(true).build());
    try {
      stub.start(request);
    } catch (Throwable t) {
      log.error("Unable to start server.", t);
      System.exit(1);
    }
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.run(request -> {
      boolean publisher = Arrays.stream(args).anyMatch(x -> x.startsWith("publish"));
      return new RemoteTask(request, publisher ? MetricsHandler.MetricName.PUBLISH_ACK_LATENCY
          : MetricsHandler.MetricName.END_TO_END_LATENCY);
    });
  }

  @Override
  public void run() {
    try {
      LoadtestProto.CheckResponse response =
          stub.check(LoadtestProto.CheckRequest.getDefaultInstance());
      numberOfMessages.addAndGet(response.getBucketValuesCount());
      response.getBucketValuesList().forEach(metricsHandler::recordLatency);
    } catch (Throwable t) {
      log.error("Error running command on remote task.", t);
    }
  }
}
