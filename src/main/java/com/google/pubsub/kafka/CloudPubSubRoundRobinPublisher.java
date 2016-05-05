package com.google.pubsub.kafka;

import com.google.common.util.concurrent.ListenableFuture;

import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;

import java.util.ArrayList;
import java.util.List;

public class CloudPubSubRoundRobinPublisher implements CloudPubSubPublisher {
  private List<CloudPubSubPublisher> publishers;
  private int currentPublisherIndex = 0;

  CloudPubSubRoundRobinPublisher(int publisherCount) {
    publishers  = new ArrayList(publisherCount);
    for (int i = 0; i < publisherCount; ++i) {
      publishers.add(new CloudPubSubGRPCPublisher());
    }
  }

  @Override
  public ListenableFuture<PublishResponse> publish(PublishRequest request) {
    currentPublisherIndex = (currentPublisherIndex + 1) % publishers.size();
    return publishers.get(currentPublisherIndex).publish(request);
  }
}
