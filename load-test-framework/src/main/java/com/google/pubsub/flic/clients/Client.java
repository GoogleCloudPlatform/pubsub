package com.google.pubsub.flic.clients;

import com.google.protobuf.Empty;
import com.google.pubsub.flic.common.Command;
import com.google.pubsub.flic.common.LoadtestFrameworkGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by maxdietz on 9/27/16.
 */
public class Client {
  public enum ClientType {
    CPS_VENEER("veneer"),
    CPS_GRPC("grpc"),
    KAFKA("kafka");

    private final String text;

    private ClientType(final String text) {
      this.text = text;
    }

    @Override
    public String toString() {
      return text;
    }
  }

  public enum ClientStatus {
    NONE,
    RUNNING,
    STOPPING,
    FAILED,
  }

  private static final Logger log = LoggerFactory.getLogger(Client.class.getName());
  private final ClientType clientType;
  private String networkAddress;
  private ClientStatus clientStatus;

  public Client(ClientType clientType, String networkAddress) {
    this.clientType = clientType;
    this.networkAddress = networkAddress;
    this.clientStatus = ClientStatus.NONE;
  }

  public ClientStatus clientStatus() {
    return clientStatus;
  }

  public ClientType clientType() {
    return clientType;
  }

  public void setNetworkAddress(String networkAddress) {
    this.networkAddress = networkAddress;
  }

  public void start() {
    // Send a gRPC call to start the server
    // select port? 5000 always a default? set this somewhere
    ManagedChannel channel = ManagedChannelBuilder.forAddress(networkAddress, 5000).usePlaintext(true).build();

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
