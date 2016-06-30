package com.google.pubsub.kafka.source;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;

/**
 * Created by rramkumar on 6/29/16.
 */
public interface CloudPubSubSubscriber {

  public ListenableFuture<PullResponse> pull(PullRequest request);
}
