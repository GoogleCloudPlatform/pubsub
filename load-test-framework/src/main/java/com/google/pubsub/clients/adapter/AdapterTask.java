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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.flic.common.LoadtestProto;
import com.google.pubsub.flic.common.LoadtestWorkerGrpc;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Task that proxies commands to a LoadtestWorker process on localhost.
 */
public class AdapterTask extends Task {
  private static final Logger log = LoggerFactory.getLogger(AdapterTask.class);
  private LoadtestWorkerGrpc.LoadtestWorkerBlockingStub stub;

  public AdapterTask(
      LoadtestProto.StartRequest request, MetricsHandler.MetricName metricName, Options options) {
    super(request.getProject(), "adapter", metricName);
    stub =
        LoadtestWorkerGrpc.newBlockingStub(
            ManagedChannelBuilder.forAddress("localhost", options.workerPort)
                .usePlaintext(true)
                .build());
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

  /**
   * Contains the command line options for {@link AdapterTask}.
   */
  @Parameters(separators = "=")
  public static class Options {
    @Parameter(
      names = {"--worker_port"},
      description = "The port that the LoadtestWorker process server is listening on."
    )
    int workerPort = 6000;
  }
}
