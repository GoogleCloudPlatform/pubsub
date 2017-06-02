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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

/** Some sensible samples that demonstrate how to use the {@link Publisher}. */
public class PublisherSamples {

  private PublisherSamples() {}

  @SuppressWarnings("unchecked")
  public static void publish(String topic, String message) throws Exception {
    Publisher publisher = Publisher.Builder.newBuilder(topic).build();

    Futures.addCallback(
        publisher.publish(
            PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(message))
                .build()),
        new FutureCallback<String>() {
          @Override
          public void onSuccess(String messageId) {
            System.out.println("Message " + messageId + " published successfully.");
          }

          @Override
          public void onFailure(Throwable thrown) {
            System.out.println("Failed to publish message: " + thrown);
          }
        });
    publisher.shutdown();
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("You must specify a topic and the message to publish as parameters.");
      System.exit(1);
      return;
    }

    PublisherSamples.publish(args[0], args[1]);
  }
}
