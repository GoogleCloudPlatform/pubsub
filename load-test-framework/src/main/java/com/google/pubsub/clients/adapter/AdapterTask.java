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
package com.google.pubsub.clients.adapter;

import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.flic.common.AdapterGrpc;
import com.google.pubsub.flic.common.LoadtestProto;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Runs a task that executes command on the adapter server.
 */
class AdapterTask extends Task {
  private static final Logger log = LoggerFactory.getLogger(AdapterTask.class);
  private AdapterGrpc.AdapterBlockingStub stub;

  AdapterTask(LoadtestProto.StartRequest request, MetricsHandler.MetricName metricName) {
    super(request.getProject(), "adapter", metricName);
    stub = AdapterGrpc.newBlockingStub(
        ManagedChannelBuilder.forAddress("localhost", 6000).usePlaintext(true).build());
    try {
      stub.start(request);
    } catch (Throwable t) {
      log.error("Unable to start server.", t);
      System.exit(1);
    }
  }

  @Override
  public void run() {
    try {
      LoadtestProto.ExecuteResponse response =
          stub.execute(LoadtestProto.ExecuteRequest.getDefaultInstance());
      numberOfMessages.addAndGet(response.getLatenciesCount());
      response.getLatenciesList().forEach(metricsHandler::recordLatency);
    } catch (Throwable t) {
      log.error("Error running command on adapter task.", t);
    }
  }
}
