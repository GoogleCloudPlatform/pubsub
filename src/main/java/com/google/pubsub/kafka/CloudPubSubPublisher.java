package com.google.pubsub.kafka;

import com.google.common.util.concurrent.ListenableFuture;

import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;

public interface CloudPubSubPublisher {
  public ListenableFuture<PublishResponse> publish(PublishRequest request);
}
