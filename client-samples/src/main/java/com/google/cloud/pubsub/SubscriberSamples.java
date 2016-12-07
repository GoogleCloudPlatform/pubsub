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

import com.google.cloud.pubsub.Subscriber.MessageReceiver;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.v1.PubsubMessage;

/** Some sensible samples that demonstrate how to use the {@link Subscriber}. */
public class SubscriberSamples {

  private SubscriberSamples() {}

  @SuppressWarnings("unchecked")
  public static void subscribe(String subscription) throws Exception {
    Subscriber.Builder.newBuilder(
            subscription,
            new MessageReceiver() {
              @Override
              public ListenableFuture<AckReply> receiveMessage(PubsubMessage message) {
                System.out.println(message.getData());
                return Futures.immediateFuture(AckReply.ACK);
              }
            })
        .build()
        .startAsync()
        .awaitTerminated();
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("You must specify a subscription path of the form "
                         + "projects/{project}/subscriptions/{subscription} as a parameter.");
      System.exit(1);
      return;
    }

    SubscriberSamples.subscribe(args[0]);
  }
}
