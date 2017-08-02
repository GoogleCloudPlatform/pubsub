package com.google.pubsub.common;

import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherBlockingStub;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberBlockingStub;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import java.util.concurrent.TimeUnit;

public class StubCreator {

  private ChannelUtil channelUtil;

  public StubCreator(ChannelUtil channelUtil) {
    this.channelUtil = channelUtil;
  }

  public SubscriberBlockingStub getSubscriberBlockingStub() {
    return SubscriberGrpc.newBlockingStub(channelUtil.getChannel())
        .withCallCredentials(channelUtil.getCallCredentials());
  }

  public SubscriberFutureStub getSubscriberFutureStub() {
    return SubscriberGrpc.newFutureStub(channelUtil.getChannel())
        .withCallCredentials(channelUtil.getCallCredentials());
  }

  public SubscriberFutureStub getSubscriberFutureStub(long timeoutMilliseconds) {
    return SubscriberGrpc.newFutureStub(channelUtil.getChannel())
        .withCallCredentials(channelUtil.getCallCredentials())
        .withDeadlineAfter(timeoutMilliseconds, TimeUnit.MILLISECONDS);
  }

  public PublisherBlockingStub getPublisherBlockingStub() {
    return PublisherGrpc.newBlockingStub(channelUtil.getChannel())
        .withCallCredentials(channelUtil.getCallCredentials());
  }
}
