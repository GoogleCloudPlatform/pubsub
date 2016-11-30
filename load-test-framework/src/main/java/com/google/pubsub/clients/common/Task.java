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

package com.google.pubsub.clients.common;

import com.google.pubsub.flic.common.LoadtestProto.MessageIdentifier;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Each task is responsible for implementing its action and for creating {@link LoadTestRunner}.
 */
public abstract class Task implements Runnable {
  protected final MetricsHandler metricsHandler;
  private AtomicInteger numMessages = new AtomicInteger(0);
  private final List<MessageIdentifier> identifiers = new ArrayList<>();
  private final AtomicLong lastUpdateMillis = new AtomicLong(System.currentTimeMillis());

  protected Task(String project, String type, MetricsHandler.MetricName metricName) {
    this.metricsHandler = new MetricsHandler(project, type, metricName);
  }

  List<Long> getBucketValues() {
    return metricsHandler.flushBucketValues();
  }

  protected void addNumberOfMessages(int toAdd) {
    numMessages.getAndAdd(toAdd);
    lastUpdateMillis.set(System.currentTimeMillis());
  }

  protected int getNumberOfMessages() {
    return numMessages.get();
  }

  protected int getAndIncrementNumberOfMessages() {
    return numMessages.getAndIncrement();
  }

  long getLastUpdateMillis() {
    return lastUpdateMillis.get();
  }

  protected synchronized void addMessageIdentifier(int clientId, int sequenceNumber) {
    identifiers.add(MessageIdentifier.newBuilder()
        .setPublisherClientId(clientId)
        .setSequenceNumber(sequenceNumber)
        .build());
    lastUpdateMillis.set(System.currentTimeMillis());
  }

  protected synchronized void addAllMessageIdentifiers(List<MessageIdentifier> identifiers) {
    this.identifiers.addAll(identifiers);
    lastUpdateMillis.set(System.currentTimeMillis());
  }

  synchronized List<MessageIdentifier> getMessageIdentifiers() {
    List<MessageIdentifier> returnedMessageIdentifiers = new ArrayList<>();
    returnedMessageIdentifiers.addAll(identifiers);
    identifiers.clear();
    return returnedMessageIdentifiers;
  }
}
