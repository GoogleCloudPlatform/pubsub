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

import com.beust.jcommander.JCommander;
import com.google.pubsub.clients.common.LoadTestRunner;
import com.google.pubsub.clients.common.MetricsHandler;
import com.google.pubsub.flic.common.LoadtestProto;

/**
 * A Task that proxies subscriber commands to a LoadtestWorker process on localhost.
 */
class SubscriberAdapterTask extends AdapterTask {

  private SubscriberAdapterTask(LoadtestProto.StartRequest request, AdapterTask.Options options) {
    super(request, MetricsHandler.MetricName.END_TO_END_LATENCY, options);
  }

  public static void main(String[] args) throws Exception {
    LoadTestRunner.Options loadtestOptions = new LoadTestRunner.Options();
    AdapterTask.Options adapterOptions = new AdapterTask.Options();
    new JCommander(loadtestOptions, args);
    new JCommander(adapterOptions, args);
    LoadTestRunner.run(
        loadtestOptions, (request) -> new SubscriberAdapterTask(request, adapterOptions));
  }
}
