/*
 * Copyright 2016 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pubsub;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.Subscriber.MessageReceiver;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service.Listener;
import com.google.common.util.concurrent.Service.State;
import com.google.pubsub.v1.PubsubMessage;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Some sensible samples that demonstrate how to use the {@link Subscriber}. */
public class SubscriberSamples {
  private final String subscription;

  private SubscriberSamples(String subscription) {
    this.subscription = subscription;
  }

  @SuppressWarnings("unchecked")
  public void simpleSubscriber(GoogleCredentials creds) throws Exception {
    final SampleMessageReceiver messageReceiver = new SampleMessageReceiver();
    Subscriber subscriber =
        Subscriber.Builder.newBuilder(subscription, creds, messageReceiver).build();

    printMessagesReceivedCount(messageReceiver);

    subscriber.startAsync();

    subscriber.addListener(
        new Listener() {
          @Override
          public void failed(State from, Throwable failure) {
            System.out.println("The subscriber has stopped with failure: " + failure);
          }
        },
        Executors.newSingleThreadExecutor());
  }

  private void printMessagesReceivedCount(final SampleMessageReceiver messageReceiver) {
    Executors.newSingleThreadScheduledExecutor()
        .scheduleAtFixedRate(
            new Runnable() {
              @Override
              public void run() {
                System.out.println("Messages received: " + messageReceiver.messagesCount.get());
              }
            },
            1,
            1,
            TimeUnit.SECONDS);
  }

  private static class SampleMessageReceiver implements MessageReceiver {
    public AtomicInteger messagesCount = new AtomicInteger();

    @Override
    public ListenableFuture<AckReply> receiveMessage(PubsubMessage message) {
      messagesCount.addAndGet(1);
      return Futures.immediateFuture(AckReply.ACK);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("You must specify a subscription as a parameter.");
      System.exit(1);
      return;
    }

    GoogleCredentials creds =
        GoogleCredentials.getApplicationDefault()
            .createScoped(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"));

    SubscriberSamples samples = new SubscriberSamples(args[0]);
    samples.simpleSubscriber(creds);
  }
}
