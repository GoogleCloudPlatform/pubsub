// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////
package com.google.pubsub.flic.cps;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.pubsub.flic.common.Utils;
import com.google.pubsub.flic.kafka.KafkaConsumerTask;
import com.google.pubsub.flic.processing.MessageProcessingHandler;
import com.google.pubsub.flic.task.CPSTask;
import com.google.pubsub.flic.task.TaskArgs;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs a publishing task that sends messages to Cloud Pub/Sub using gRPC. */
public class CPSPublishingTask extends CPSTask {
  private static final Logger log = LoggerFactory.getLogger(KafkaConsumerTask.class.getName());

  private CPSRoundRobinPublisher publisher;
  private MessageProcessingHandler processingHandler;

  public CPSPublishingTask(
      TaskArgs args, CPSRoundRobinPublisher publisher, MessageProcessingHandler processingHandler) {
    super(args);
    this.publisher = publisher;
    this.processingHandler = processingHandler;
  }

  /** Publishes messages to Cloud Pub/Sub in the most optimal way possible */
  public void execute() throws Exception {
    List<String> topics = args.getTopics();
    String cpsProject = args.getCPSProject();
    // The base message which will have a number appended to it.
    String baseMessage = Utils.createMessage(args.getMessageSize(), 0);
    PubsubMessage.Builder messageBuilder = PubsubMessage.newBuilder();
    PublishRequest.Builder requestBuilder = PublishRequest.newBuilder();
    // Warm up all threads in the thread pool.
    callbackExecutor.prestartAllCoreThreads();
    // Get the overall start time.
    long start = System.currentTimeMillis();
    while (messageNo.intValue() <= args.getNumMessages() && !failureFlag.get()) {
      List<PubsubMessage> batch = new ArrayList<>();
      // Keep track of the total # of bytes in this batch and the size of the batch.
      AtomicLong batchBytes = new AtomicLong(0);
      AtomicInteger counter = new AtomicInteger(1);
      for (; counter.intValue() <= args.getBatchSize(); counter.incrementAndGet()) {
        String messageToSend = baseMessage + messageNo;
        batchBytes.addAndGet(messageToSend.getBytes().length);
        Map<String, String> attributes = new HashMap<>();
        attributes.put(Utils.KEY_ATTRIBUTE, String.valueOf(messageNo));
        // Get the timestamp for each message in the batch. Used for end-to-end latency.
        String timestamp = String.valueOf(System.currentTimeMillis());
        attributes.put(Utils.TIMESTAMP_ATTRIBUTE, timestamp);
        PubsubMessage message =
            getPubsubMessage(
                ByteString.copyFrom(messageToSend.getBytes()), attributes, messageBuilder);
        batch.add(message);
        MessageProcessingHandler.displayProgress(marker, messageNo);
        if (messageNo.incrementAndGet() > args.getNumMessages()) {
          counter.incrementAndGet();
          break;
        }
      }
      // Throttles the rate of making a PublishRequest if necessary.
      rateLimiter.acquire();
      String cpsTopic =
          Utils.getCPSTopic(topics.get(messageNo.intValue() % topics.size()), cpsProject);
      PublishRequest request = getPublishRequest(cpsTopic, requestBuilder, batch);
      ListenableFuture<PublishResponse> responseFuture = publisher.publish(request);
      // Get the publishing timestamp of the batch.
      long batchTimestamp = System.currentTimeMillis();
      Futures.addCallback(
          responseFuture,
          new FutureCallback<PublishResponse>() {
            @Override
            public void onSuccess(PublishResponse result) {
              if (!failureFlag.get()) {
                // Each message in the batch gets the same pub-to-ack time based on the
                // batch timestamp acquired earlier.
                long latency = System.currentTimeMillis() - batchTimestamp;
                processingHandler.addStats(counter.intValue() - 1, latency, batchBytes.longValue());
              }
            }

            @Override
            public void onFailure(Throwable t) {
              if (!failureFlag.get()) {
                // Indicates that a PublishRequest failed to all other callback threads and the
                // main thread.
                failureFlag.set(true);
                log.error(t.getMessage(), t);
                processingHandler.wakeUpCondition();
              }
            }
          },
          callbackExecutor);
      requestBuilder.clear();
      messageBuilder.clear();
    }
    log.info("Waiting for all acks to arrive...");
    processingHandler.printStats(start, callbackExecutor, failureFlag);
    callbackExecutor.shutdownNow();
    log.info("Done!");
  }

  /** Create a PubsubMessage from data and some attributes. */
  private PubsubMessage getPubsubMessage(
      ByteString data, Map<String, String> attributes, PubsubMessage.Builder builder) {
    PubsubMessage message = builder.setData(data).putAllAttributes(attributes).build();
    builder.clear();
    return message;
  }

  /** Creates a PublishRequest from a Cloud Pub/Sub topic and a list of messages. */
  private PublishRequest getPublishRequest(
      String topic, PublishRequest.Builder builder, List<PubsubMessage> messages) {
    PublishRequest request = builder.setTopic(topic).addAllMessages(messages).build();
    builder.clear();
    return request;
  }
}
