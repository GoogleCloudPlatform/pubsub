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
package com.google.pubsub.flic.task;

import java.util.List;

/** The necessary arguments for each {@Task} subclass. */
public class TaskArgs {

  private String broker;
  private List<String> topics;
  private String cpsProject;
  private int batchSize;
  private int numMessages;
  private int messageSize;
  private int numResponseThreads;
  private int rateLimit;
  private String subscription;
  private int maxMessagesPerPull;
  private int maxOpenPullsPerSubscription;

  public TaskArgs(TaskArgsBuilder builder) {
    this.broker = builder.broker;
    this.topics = builder.topics;
    this.cpsProject = builder.cpsProject;
    this.numMessages = builder.numMessages;
    this.messageSize = builder.messageSize;
    this.batchSize = builder.batchSize;
    this.numResponseThreads = builder.numResponseThreads;
    this.rateLimit = builder.rateLimit;
  }

  public String getBroker() {
    return broker;
  }

  public List<String> getTopics() {
    return topics;
  }

  public String getCPSProject() {
    return cpsProject;
  }

  public int getNumMessages() {
    return numMessages;
  }

  public int getMessageSize() {
    return messageSize;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public int getNumResponseThreads() {
    return numResponseThreads;
  }

  public int getRateLimit() {
    return rateLimit;
  }

  public String getSubscription() {
    return subscription;
  }

  public int getMaxMessagesPerPull() {
    return maxMessagesPerPull;
  }

  /** Builder class for {@link TaskArgs} */
  public static class TaskArgsBuilder {

    private String broker;
    private List<String> topics;
    private String cpsProject;
    private int numMessages;
    private int messageSize;
    private int batchSize;
    private int numResponseThreads;
    private int rateLimit;

    public TaskArgsBuilder topics(List<String> topics) {
      this.topics = topics;
      return this;
    }

    public TaskArgsBuilder cpsProject(String cpsProject) {
      this.cpsProject = cpsProject;
      return this;
    }

    public TaskArgsBuilder broker(String broker) {
      this.broker = broker;
      return this;
    }

    public TaskArgsBuilder numMessages(int numMessages) {
      this.numMessages = numMessages;
      return this;
    }

    public TaskArgsBuilder messageSize(int messageSize) {
      this.messageSize = messageSize;
      return this;
    }

    public TaskArgsBuilder batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public TaskArgsBuilder numResponseThreads(int numResponseThreads) {
      this.numResponseThreads = numResponseThreads;
      return this;
    }

    public TaskArgsBuilder rateLimit(int rateLimit) {
      this.rateLimit = rateLimit;
      return this;
    }

    public TaskArgs build() {
      return new TaskArgs(this);
    }
  }
}
