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
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.Firewall;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirewallResourceController extends ResourceController {
  private static final Logger log = LoggerFactory.getLogger(FirewallResourceController.class);
  private static String FIREWALL_NAME = "cloud-loadtest-framework-firewall-rule";

  private final String project;
  private final Compute compute;

  public FirewallResourceController(
      String project, ScheduledExecutorService executor, Compute compute) {
    super(executor);
    this.project = project;
    this.compute = compute;
  }

  @Override
  protected void startAction() throws Exception {
    log.info("Creating firewall");
    Firewall firewallRule =
        new Firewall()
            .setName(FIREWALL_NAME)
            .setDescription(
                "A firewall rule to allow the driver to coordinate load test instances.")
            .setAllowed(
                ImmutableList.of(
                    new Firewall.Allowed()
                        .setIPProtocol("tcp")
                        .setPorts(Collections.singletonList("5000"))));
    try {
      compute.firewalls().insert(project, firewallRule).execute();
    } catch (GoogleJsonResponseException e) {
      log.info("Firewall error: " + e);
      if (e.getStatusCode() != HttpStatus.SC_CONFLICT) {
        throw e;
      }
    }
  }

  @Override
  protected void stopAction() throws Exception {
    log.info("Cleaning up firewall resource_controllers.");
    compute.firewalls().delete(project, FIREWALL_NAME);
    log.info("Cleaned up firewall resource_controllers.");
  }
}
