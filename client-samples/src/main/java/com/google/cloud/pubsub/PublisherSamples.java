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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.joda.time.Duration;

/**
 * Some sensible samples that demonstrate how to use the {@link Publisher Cloud Pub/Sub Publisher}.
 */
public class PublisherSamples {
  private final String topic;
  private long startTimeMs;

  private PublisherSamples(String topic) {
    this.topic = topic;
  }

  @SuppressWarnings("unchecked")
  public void simplePublisher() throws Exception {
    Publisher publisher =
        Publisher.Builder.newBuilder(topic)
            .setMaxBatchDuration(new Duration(500))
            .setMaxBatchBytes(1 * 1000 * 1000)
            .setRequestTimeout(Duration.standardSeconds(60))
            .build();

    startTimeMs = System.currentTimeMillis();
    System.out.println("Publishing messages at " + startTimeMs + " ms.");

    List<ListenableFuture<String>> results = new ArrayList<>();

    for (int i = 0; i < 1000; ++i) {
      PubsubMessage message =
          PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("" + i)).build();
      results.add(publisher.publish(message));
    }

    System.out.println(
        "Batched messages in " + (System.currentTimeMillis() - startTimeMs) + " ms.");

    handleResults(results);

    publisher.shutdown();
  }

  public void customBatchingPublisher() throws Exception {
    Publisher publisher =
        Publisher.Builder.newBuilder(topic)
            .setMaxBatchMessages(10)
            .build();

    List<ListenableFuture<String>> results = new ArrayList<>();
    PubsubMessage message =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(String.format("my default batch options message")))
            .build();
    // Uses the default batching options.
    results.add(publisher.publish(message));

    handleResults(results);
    publisher.shutdown();
  }

  private void flowControlledPublisher() throws Exception {
    Publisher publisher =
        Publisher.Builder.newBuilder(topic)
            .setMaxOutstandingBytes(10 * 1000 * 1000) // 10 MB
            .build();

    List<ListenableFuture<String>> results = new ArrayList<>();
    PubsubMessage message =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(String.format("my default batch options message")))
            .build();
    // Uses the default batching options.
    results.add(publisher.publish(message));

    handleResults(results);
    publisher.shutdown();
  }

  private void handleResults(List<ListenableFuture<String>> results) {
    List<String> messageIds = new ArrayList<>(results.size());
    Futures.whenAllComplete(results)
        .call(
            new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                long endTime = System.currentTimeMillis();
                System.out.printf(
                    "Finished publishing messages at %d ms, elapsed %d ms\n",
                    endTime, endTime - startTimeMs);
                int okCount = 0;
                int failCount = 0;
                for (ListenableFuture<String> result : results) {
                  try {
                    messageIds.add(result.get());
                    ++okCount;
                  } catch (Exception e) {
                    e.printStackTrace();
                    ++failCount;
                  }
                }
                System.out.println("Total messages published OK: " + okCount);
                System.out.println("Total messages failed: " + failCount);

                return null;
              }
            });
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("You must specify a topic as a parameter.");
      System.exit(1);
      return;
    }

    PublisherSamples samples = new PublisherSamples(args[0]);
    samples.simplePublisher();
    samples.customBatchingPublisher();
    samples.flowControlledPublisher();
  }
}
