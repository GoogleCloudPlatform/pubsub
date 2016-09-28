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

import com.google.protobuf.Empty;
import com.google.pubsub.flic.common.Command;
import com.google.pubsub.flic.common.LoadtestFrameworkGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Client {
  enum ClientType {
    CPS_VENEER("veneer"),
    CPS_GRPC("grpc"),
    KAFKA("kafka");

    private final String text;

    ClientType(final String text) {
      this.text = text;
    }

    @Override
    public String toString() {
      return text;
    }
  }

  private enum ClientStatus {
    NONE,
    RUNNING,
    STOPPING,
    FAILED,
  }

  private static final Logger log = LoggerFactory.getLogger(Client.class.getName());
  private final ClientType clientType;
  private String networkAddress;
  private ClientStatus clientStatus;
  private static final int port = 5000;

  Client(ClientType clientType, String networkAddress) {
    this.clientType = clientType;
    this.networkAddress = networkAddress;
    this.clientStatus = ClientStatus.NONE;
  }

  public ClientStatus clientStatus() {
    return clientStatus;
  }

  ClientType clientType() {
    return clientType;
  }

  public void setNetworkAddress(String networkAddress) {
    this.networkAddress = networkAddress;
  }

  void start() {
    // Send a gRPC call to start the server
    ManagedChannel channel = ManagedChannelBuilder.forAddress(networkAddress, port).usePlaintext(true).build();

    LoadtestFrameworkGrpc.LoadtestFrameworkStub stub = LoadtestFrameworkGrpc.newStub(channel);
    Command.CommandRequest request = Command.CommandRequest.newBuilder().build();
    stub.startClient(request, new StreamObserver<Empty>() {
      @Override
      public void onNext(Empty empty) {
        log.info("Successfully started client [" + networkAddress + "]");
        clientStatus = ClientStatus.RUNNING;
      }

      @Override
      public void onError(Throwable throwable) {
        clientStatus = ClientStatus.FAILED;
      }

      @Override
      public void onCompleted() {
      }
    });
  }
}
