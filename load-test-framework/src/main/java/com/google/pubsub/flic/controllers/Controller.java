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

package com.google.pubsub.flic.controllers;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.pubsub.flic.common.LatencyDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

abstract class Controller {
  final static Logger log = LoggerFactory.getLogger(Controller.class);
  final List<Client> clients = new ArrayList<>();
  final ScheduledExecutorService executor;

  Controller(ScheduledExecutorService executor) {
    this.executor = executor;
  }

  /*
  Creates the given environments and starts the virtual machines. When this function returns, each client is guaranteed
  to have been connected and be network reachable, but is not started. If an error occurred attempting to start the
  environment, the environment will be shut down, and an IOException will be thrown. It is not guaranteed that we have
  completed shutting down when this function returns, but it is guaranteed that we are in process.
   */
  public abstract void initialize() throws Throwable;

  protected abstract void shutdown(Throwable t);

  public Map<Client.ClientType, long[]> getResults() {
    try {
      List<ListenableFuture<Void>> doneFutures = new ArrayList<>();
      clients.forEach(c -> doneFutures.add(c.getDoneFuture()));
      Futures.allAsList(doneFutures).get();
    } catch (InterruptedException | ExecutionException e) {
      log.error("Client failed health check, will print results accumulated during test up to this point.",
          e instanceof ExecutionException ? e.getCause() : e);
    }
    Map<Client.ClientType, long[]> results = new HashMap<>();
    for (Client.ClientType type : Client.ClientType.values()) {
      results.put(type, new long[LatencyDistribution.LATENCY_BUCKETS.length]);
    }
    clients.forEach(client -> {
      for (int i = 0; i < LatencyDistribution.LATENCY_BUCKETS.length; i++) {
        results.get(client.getClientType())[i] += client.getBucketValues()[i];
      }
    });
    return results;
  }

  void startClients() {
    SettableFuture<Void> startFuture = SettableFuture.create();
    clients.forEach((client) -> executor.execute(() -> {
      try {
        client.start();
        startFuture.set(null);
      } catch (Throwable t) {
        startFuture.setException(t);
      }
    }));
    try {
      startFuture.get();
    } catch (ExecutionException e) {
      shutdown(e.getCause());
    } catch (InterruptedException e) {
      shutdown(e);
    }
  }
}

