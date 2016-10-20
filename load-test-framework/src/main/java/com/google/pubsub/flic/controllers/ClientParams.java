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

public class ClientParams {
  final String subscription;
  private final Client.ClientType clientType;

  public ClientParams(Client.ClientType clientType, String subscription) {
    this.clientType = clientType;
    this.subscription = subscription;
  }

  /**
   * @return the clientType
   */
  public Client.ClientType getClientType() {
    return clientType;
  }
}