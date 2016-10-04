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

import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

abstract class Controller {
  final List<Client> clients = new ArrayList<>();
  final Executor executor;

  Controller(Executor executor) {
    this.executor = executor;
  }

  /*
  Creates the given environments and starts the virtual machines. When this function returns, each client is guaranteed
  to have been connected and be network reachable, but is not started. If an error occurred attempting to start the
  environment, the environment will be shut down, and an IOException will be thrown. It is not guaranteed that we have
  completed shutting down when this function returns, but it is guaranteed that we are in process.
   */
  public abstract void initialize() throws Throwable;

  public abstract void shutdown(Throwable t);

  public void startClients() {
    SettableFuture<Void> startFuture = SettableFuture.create();
    clients.forEach((client) -> executor.execute(() -> {
      try {
        client.start();
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

