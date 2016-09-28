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

import com.google.pubsub.flic.clients.Client;

import java.io.IOException;
import java.util.List;

public abstract class Controller {
  private List<Client> clients;

  /*
  Creates the given environments and starts the virtual machines. When this function returns, each client is guaranteed
  to have been connected and be network reachable, but is not started. If an error occurred attempting to start the
  environment, the environment will be shut down, and an IOException will be thrown.
   */
  abstract void initialize() throws IOException;
  abstract void shutdown(Throwable t);
  void startClients() {
    clients.forEach(Client::start);
  }
}

