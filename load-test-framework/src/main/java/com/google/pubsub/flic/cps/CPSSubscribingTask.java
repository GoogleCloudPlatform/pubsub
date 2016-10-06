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

import com.beust.jcommander.ParameterException;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.flic.common.Utils;
import com.google.pubsub.flic.common.MessagePacketProto.MessagePacket;
import com.google.pubsub.flic.kafka.KafkaConsumerTask;
import com.google.pubsub.flic.processing.MessageProcessingHandler;
import com.google.pubsub.flic.task.CPSTask;
import com.google.pubsub.flic.task.TaskArgs;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs a subscribing task that consumes messages from Cloud Pub/Sub using gRPC. */
public class CPSSubscribingTask extends CPSTask {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumerTask.class.getName());
  private static final String KAFKA_TOPIC_ATTRIBUTE = "kafka_topic";
  private static final int MAX_MESSAGES_PER_PULL = 1000;
  // TODO(rramkumar): Make this a user arg?
  private static final int MAX_OPEN_PULLS_PER_SUBSCRIPTION = 1000;

  private CPSRoundRobinSubscriber subscriber;
  private MessageProcessingHandler processingHandler;
  private long earliestReceived;
  private ReentrantReadWriteLock lock;

  public CPSSubscribingTask(
      TaskArgs args, CPSRoundRobinSubscriber subscriber, MessageProcessingHandler processingHandler) {
    super(args);
    this.subscriber = subscriber;
    this.processingHandler = processingHandler;
    earliestReceived = Long.MAX_VALUE;
    lock = new ReentrantReadWriteLock();
  }

  /** Consumes messages from Cloud Pub/Sub in the most optimal way possible */
  public void execute() throws Exception {
    // Store each subscription we create.
    List<Subscription> subscriptions = new ArrayList<>();
    // Keep track of the number of open pull requests per subscription.
    Map<Subscription, MutableInt> openPullsPerSubscription = new ConcurrentHashMap<>();
    // Store ack ids for each message we received in the callbacks.
    List<String> ackIds = Collections.synchronizedList(new ArrayList<>());
    Subscription.Builder subBuilder = Subscription.newBuilder();
    AcknowledgeRequest.Builder ackBuilder = AcknowledgeRequest.newBuilder();
    // Create a subscription for each topic.
    for (String topic : args.getTopics()) {
      String[] splitOnColon = topic.split(":");
      if(splitOnColon.length != 2)
        throw new ParameterException("Parameter topics should be formatted 'topic:subscription'");
      subBuilder.setName(Utils.getCPSSubscription(splitOnColon[1], args.getCPSProject()))
                .setTopic(Utils.getCPSTopic(splitOnColon[0], args.getCPSProject()));
      Subscription request = subBuilder.build();
      Subscription response;
      try   {
        response = subscriber.createSubscription(request).get();
      } catch(ExecutionException e)  {
        Throwable cause = e.getCause();
        // Check to see if creating the subscription failed bc subscription already existed
        if (cause instanceof StatusRuntimeException && 
            ((StatusRuntimeException) cause).getStatus().getCode().equals(Status.ALREADY_EXISTS.getCode())) {
          subscriber.deleteSubscription(request).get();
          response = subscriber.createSubscription(request).get();
        }
        else throw e;
      }
      subscriptions.add(response);
      openPullsPerSubscription.put(response, new MutableInt(0));
    }
    log.info("Start publishing...");
    // Get the overall start time.
    PullRequest.Builder pullBuilder = PullRequest.newBuilder();
    while (messageNo.intValue() <= args.getNumMessages() && !failureFlag.get()) {
      for (Subscription s : subscriptions) {
        // Only continue if we have not hit the max number of open pull requests for this
        // subscription.
        if (openPullsPerSubscription.get(s).intValue() == MAX_OPEN_PULLS_PER_SUBSCRIPTION) {
          continue;
        }
        // Throttles the rate of making a PullRequest if necessary.
        rateLimiter.acquire();
        openPullsPerSubscription.get(s).increment();
        PullRequest request = getPullRequest(s.getName(), pullBuilder);
        ListenableFuture<PullResponse> response = subscriber.pull(request);
        Futures.addCallback(
            response,
            new FutureCallback<PullResponse>() {
              @Override
              public void onSuccess(PullResponse response) {
                openPullsPerSubscription.get(s).decrement();
                Iterator<ReceivedMessage> receivedMessageIterator =
                    response.getReceivedMessagesList().iterator();
                // Each message in this callback gets received at the "same time".
                long receivedTime = System.currentTimeMillis();
                // Check if this is the first received message, if so, change variable
                lock.readLock().lock();
                if (receivedTime < earliestReceived) {
                  lock.writeLock().lock(); // Should only lock on first block received
                  earliestReceived = receivedTime;
                  lock.writeLock().unlock();
                }
                lock.readLock().unlock();
                while (receivedMessageIterator.hasNext()
                    && !failureFlag.get()
                    && messageNo.intValue() <= args.getNumMessages()) {
                  ReceivedMessage rm = receivedMessageIterator.next();
                  PubsubMessage message = rm.getMessage();
                  Map<String, String> attributes = message.getAttributes();
                  ackIds.add(rm.getAckId());
                  long latency = 0;
                  if (attributes.get(Utils.TIMESTAMP_ATTRIBUTE) != null) {
                    // Calculate end-to-end latency if this message has the appropriate attribute.
                    latency =
                        receivedTime - Long.valueOf(attributes.get(Utils.TIMESTAMP_ATTRIBUTE));
                  }
                  processingHandler.addStats(1, latency, message.getData().size());
                  if (processingHandler.getFiledump() != null) {
                    // If the user wanted to dump this data to a file, then do so.
                    String topic = extractTopic(s.getTopic());
                    if (attributes.get(KAFKA_TOPIC_ATTRIBUTE) != null) {
                      // If the message was published to Cloud Pub/Sub using the CloudPubSubConnector,
                      // then set the topic of the message appropriately.
                      topic = attributes.get(KAFKA_TOPIC_ATTRIBUTE);
                    }
                    try {
                      MessagePacket packet =
                          MessagePacket.newBuilder()
                              .setTopic(topic)
                              .setKey(attributes.get(Utils.KEY_ATTRIBUTE))
                              .setValue(message.getData().toStringUtf8())
                              .setLatency(latency)
                              .setReceivedTime(receivedTime)
                              .build();
                      processingHandler.addMessagePacket(packet);
                    } catch (Exception e) {
                      failureFlag.set(true);
                      log.error(e.getMessage(), e);
                    }
                  }
                  MessageProcessingHandler.displayProgress(marker, messageNo);
                  if (messageNo.incrementAndGet() > args.getNumMessages()) {
                    break;
                  }
                }
                synchronized (ackIds) {
                  subscriber.acknowledge(getAckRequest(s.getName(), ackBuilder, ackIds));
                  ackIds.clear();
                }
              }

              @Override
              public void onFailure(Throwable t) {
                if (!failureFlag.get()) {
                  // Indicates that a PullRequest failed to all other callback threads and the
                  // main thread.
                  failureFlag.set(true);
                  log.error(t.getMessage(), t);
                }
              }
            },
            callbackExecutor);
      }
    }
    processingHandler.printStats(earliestReceived, callbackExecutor, failureFlag);
    callbackExecutor.shutdownNow();
    log.info("Done!");
  }

  /** Creates an AcknowledgeRequest from a subscription name and a list of ack id's. */
  private AcknowledgeRequest getAckRequest(
      String subscriptionName, AcknowledgeRequest.Builder builder, List<String> ackIds) {
    AcknowledgeRequest request =
        builder.setSubscription(subscriptionName).addAllAckIds(ackIds).build();
    builder.clear();
    return request;
  }

  /** Creates a PullRequest from a subscription name. */
  private PullRequest getPullRequest(String subscriptionName, PullRequest.Builder builder) {
    PullRequest request =
        builder
            .setSubscription(subscriptionName)
            .setReturnImmediately(true)
            .setMaxMessages(MAX_MESSAGES_PER_PULL)
            .build();
    builder.clear();
    return request;
  }

  /** Extracts the single-word topic name from the full path name that Cloud Pub/Sub uses. */
  private String extractTopic(String topic) {
    return topic.substring(topic.lastIndexOf('/') + 1).trim();
  }
}
