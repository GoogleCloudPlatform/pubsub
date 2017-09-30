package com.google.pubsub.clients.consumer.ack;

import com.google.pubsub.v1.PubsubMessage;

public class MappedApiMessageReceiver implements MessageReceiver {

  @Override
  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
    consumer.ack();
  }
}
