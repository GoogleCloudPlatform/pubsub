/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.google.pubsub.flic.controllers.resource_controllers;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A ResourceController that manages cloud pubsub topics and subscriptions. */
public class PubsubResourceController extends ResourceController {
  protected static final Logger log = LoggerFactory.getLogger(PubsubResourceController.class);
  private final String project;
  private final String topic;
  private final List<String> subscriptions;
  private final Pubsub pubsub;

  public PubsubResourceController(
      String project,
      String topic,
      List<String> subscriptions,
      ScheduledExecutorService executor,
      Pubsub pubsub) {
    super(executor);
    this.project = project;
    this.topic = topic;
    this.subscriptions = subscriptions;
    this.pubsub = pubsub;
  }

  @Override
  protected void startAction() throws Exception {
    try {
      pubsub
          .projects()
          .topics()
          .create("projects/" + project + "/topics/" + topic, new Topic())
          .execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() != HttpStatus.SC_CONFLICT) {
        log.error("Error creating topic {}", topic);
        throw e;
      }
      log.info("Topic {} already exists, reusing.", topic);
    }
    for (String subscription : subscriptions) {
      log.error("Creating subscription: {}", subscription);
      try {
        pubsub
            .projects()
            .subscriptions()
            .delete("projects/" + project + "/subscriptions/" + subscription)
            .execute();
      } catch (IOException e) {
        log.debug("Error deleting subscription {}: {}", subscription, e);
      }
      pubsub
          .projects()
          .subscriptions()
          .create(
              "projects/" + project + "/subscriptions/" + subscription,
              new Subscription()
                  .setTopic("projects/" + project + "/topics/" + topic)
                  .setAckDeadlineSeconds(10))
          .execute();
    }
  }

  @Override
  protected void stopAction() throws Exception {
    log.info("Cleaning up pubsub resource_controllers.");
    for (String subscription : subscriptions) {
      pubsub
          .projects()
          .subscriptions()
          .delete("projects/" + project + "/subscriptions/" + subscription)
          .execute();
    }
    pubsub.projects().topics().delete("projects/" + project + "/topics/" + topic).execute();
    log.info("Cleaned up pubsub resource_controllers.");
  }
}
