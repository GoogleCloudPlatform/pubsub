package com.google.pubsub.kafka.source;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.SubscriberInterface;

public interface StreamingPullSubscriberFactory {
  SubscriberInterface newSubscriber(MessageReceiver receiver);
}
