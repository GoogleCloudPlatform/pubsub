package com.google.pubsub.clients.consumer;

import com.google.pubsub.common.ChannelUtil;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherBlockingStub;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberBlockingStub;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;

/**
 * Created by pietrzykp on 7/28/17.
 */
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

  public PublisherBlockingStub getPublisherBlockingStub() {
    return PublisherGrpc.newBlockingStub(channelUtil.getChannel())
        .withCallCredentials(channelUtil.getCallCredentials());
  }
}
