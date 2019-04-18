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

package com.google.pubsub.flic.controllers;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.flic.common.LatencyTracker;
import com.google.pubsub.flic.common.MessageTracker;
import com.google.pubsub.flic.controllers.resource_controllers.ComputeResourceController;
import com.google.pubsub.flic.controllers.resource_controllers.ResourceController;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ControllerBase implements Controller {
  protected static final Logger log = LoggerFactory.getLogger(Controller.class);
  protected final List<Client> clients = new ArrayList<>();
  protected final ScheduledExecutorService executor;
  private final List<ResourceController> controllers;
  private final List<ComputeResourceController> computeControllers;
  private final Map<ClientType, LatencyTracker> clientLatencyTrackers = new HashMap<>();

  private Timestamp startTime = null;

  /**
   * Creates the given environments and starts the virtual machines. When this function returns,
   * each client is guaranteed to have been connected and be network reachable, but is not started.
   * If an error occurred attempting to start the environment, the environment will be shut down,
   * and an Exception will be thrown. It is not guaranteed that we have completed shutting down when
   * this function returns, but it is guaranteed that we are in process.
   *
   * @param executor the executor that will be used to schedule all environment initialization tasks
   */
  public ControllerBase(
      ScheduledExecutorService executor,
      List<ResourceController> controllers,
      List<ComputeResourceController> computeControllers) {
    this.executor = executor;
    this.controllers = controllers;
    this.computeControllers = computeControllers;
  }

  @Override
  public void waitForClients() throws Throwable {
    try {
      Futures.allAsList(clients.stream().map(Client::getDoneFuture).collect(Collectors.toList()))
          .get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Override
  public Timestamp getStartTime() {
    return startTime;
  }

  private LatencyTracker getLatencyTrackerForType(ClientType type) {
    if (!clientLatencyTrackers.containsKey(type)) {
      clientLatencyTrackers.put(type, new LatencyTracker());
    }
    return clientLatencyTrackers.get(type);
  }

  @Override
  public Map<ClientType, LatencyTracker> getClientLatencyTrackers() {
    return clientLatencyTrackers;
  }

  @Override
  public void start(MessageTracker messageTracker) {
    // Start all ResourceControllers
    List<ListenableFuture<Void>> controllerFutures = new ArrayList<>();
    for (ResourceController controller : controllers) {
      controllerFutures.add(controller.start());
    }
    try {
      Futures.allAsList(controllerFutures).get();
    } catch (Exception e) {
      log.error("error starting controller: " + e);
      stop();
      return;
    }
    log.info("Started non-compute resource_controllers.");

    // Create all clients
    List<ListenableFuture<List<Client>>> clientFutures = new ArrayList<>();
    computeControllers.forEach(controller -> clientFutures.add(controller.startClients()));
    try {
      clients.addAll(
          Futures.allAsList(clientFutures).get().stream()
              .flatMap(Collection::stream)
              .collect(Collectors.toList()));
    } catch (Exception e) {
      log.error("error starting compute: " + e);
      stop();
      return;
    }
    log.info("Started compute resource_controllers.");

    // Start all clients
    startTime =
        Timestamps.add(
            Timestamps.fromMillis(System.currentTimeMillis()), Durations.fromSeconds(60));
    List<ListenableFuture<Void>> clientStartFutures = new ArrayList<>();
    for (Client client : clients) {
      SettableFuture<Void> future = SettableFuture.create();
      executor.execute(
          () -> {
            try {
              client.start(
                  startTime, messageTracker, getLatencyTrackerForType(client.getClientType()));
              future.set(null);
            } catch (Throwable t) {
              future.setException(t);
            }
          });
      clientStartFutures.add(future);
    }
    try {
      Futures.allAsList(clientStartFutures).get();
    } catch (Exception e) {
      log.error("error starting client: " + e);
      stop();
      return;
    }
    log.info("Started all clients.");
  }

  @Override
  public void stop() {
    ArrayList<ListenableFuture<Void>> futures = new ArrayList<>();
    for (ResourceController controller : controllers) {
      futures.add(controller.stop());
    }
    try {
      log.info("Stopping all controllers.");
      Futures.allAsList(futures).get();
      log.info("Stopped all controllers.");
    } catch (Exception e) {
      log.error("Failed to stop: " + e);
    }
  }
}
