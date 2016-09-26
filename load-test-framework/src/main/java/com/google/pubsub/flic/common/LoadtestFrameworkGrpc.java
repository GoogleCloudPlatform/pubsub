package com.google.pubsub.flic.common;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.1.0-SNAPSHOT)",
    comments = "Source: Command.proto")
public class LoadtestFrameworkGrpc {

  private LoadtestFrameworkGrpc() {}

  public static final String SERVICE_NAME = "pubsub.LoadtestFramework";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.pubsub.flic.common.Command.CommandRequest,
      com.google.protobuf.Empty> METHOD_START_CLIENT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "pubsub.LoadtestFramework", "StartClient"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.pubsub.flic.common.Command.CommandRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static LoadtestFrameworkStub newStub(io.grpc.Channel channel) {
    return new LoadtestFrameworkStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static LoadtestFrameworkBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new LoadtestFrameworkBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static LoadtestFrameworkFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new LoadtestFrameworkFutureStub(channel);
  }

  /**
   */
  public static abstract class LoadtestFrameworkImplBase implements io.grpc.BindableService {

    /**
     */
    public void startClient(com.google.pubsub.flic.common.Command.CommandRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_START_CLIENT, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_START_CLIENT,
            asyncUnaryCall(
              new MethodHandlers<
                com.google.pubsub.flic.common.Command.CommandRequest,
                com.google.protobuf.Empty>(
                  this, METHODID_START_CLIENT)))
          .build();
    }
  }

  /**
   */
  public static final class LoadtestFrameworkStub extends io.grpc.stub.AbstractStub<LoadtestFrameworkStub> {
    private LoadtestFrameworkStub(io.grpc.Channel channel) {
      super(channel);
    }

    private LoadtestFrameworkStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LoadtestFrameworkStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new LoadtestFrameworkStub(channel, callOptions);
    }

    /**
     */
    public void startClient(com.google.pubsub.flic.common.Command.CommandRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_START_CLIENT, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class LoadtestFrameworkBlockingStub extends io.grpc.stub.AbstractStub<LoadtestFrameworkBlockingStub> {
    private LoadtestFrameworkBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private LoadtestFrameworkBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LoadtestFrameworkBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new LoadtestFrameworkBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.google.protobuf.Empty startClient(com.google.pubsub.flic.common.Command.CommandRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_START_CLIENT, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class LoadtestFrameworkFutureStub extends io.grpc.stub.AbstractStub<LoadtestFrameworkFutureStub> {
    private LoadtestFrameworkFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private LoadtestFrameworkFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected LoadtestFrameworkFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new LoadtestFrameworkFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> startClient(
        com.google.pubsub.flic.common.Command.CommandRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_START_CLIENT, getCallOptions()), request);
    }
  }

  private static final int METHODID_START_CLIENT = 0;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final LoadtestFrameworkImplBase serviceImpl;
    private final int methodId;

    public MethodHandlers(LoadtestFrameworkImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_START_CLIENT:
          serviceImpl.startClient((com.google.pubsub.flic.common.Command.CommandRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    return new io.grpc.ServiceDescriptor(SERVICE_NAME,
        METHOD_START_CLIENT);
  }
}
