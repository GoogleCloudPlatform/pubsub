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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.clients.common.Task;
import com.google.pubsub.clients.common.Task.RunResult;
import com.google.pubsub.flic.common.LoadtestProto;
import com.google.pubsub.flic.common.LoadtestWorkerGrpc;
import io.grpc.ManagedChannelBuilder;


/**
 * A Task that proxies commands to a LoadtestWorker process on localhost.
 */
public class AdapterTask extends Task {
  private final LoadtestWorkerGrpc.LoadtestWorkerFutureStub stub;

  public AdapterTask(
      LoadtestProto.StartRequest request, MetricsHandler.MetricName metricName, Options options) {
    super(request, "adapter", metricName);
    stub =
        LoadtestWorkerGrpc.newFutureStub(
            ManagedChannelBuilder.forAddress("localhost", options.workerPort)
                .usePlaintext(true)
                .build());
    try {
      stub.start(request);
    } catch (Throwable t) {
      throw new RuntimeException("Unable to start server.", t);
    }
  }

  @Override
  public ListenableFuture<RunResult> doRun() {
    return Futures.transform(
        stub.execute(LoadtestProto.ExecuteRequest.getDefaultInstance()),
        response ->
            RunResult.fromMessages(
                response.getReceivedMessagesList(), response.getLatenciesList()));
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
