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
package com.google.pubsub.flic.controllers;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.storage.Storage;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is a subclass of {@link Controller} that controls load tests on Google Compute Engine.
 */
public class GCEController extends ControllerBase {
  private final Map<String, Map<ClientParams, Integer>> types;

  /**
   * Instantiates the load test on Google Compute Engine.
   */
  private GCEController(Map<String, Map<ClientParams, Integer>> types,
                        ScheduledExecutorService executor, List<ResourceController> controllers,
                        List<ComputeResourceController> computeControllers) {
    super(executor, controllers, computeControllers);
    this.types = types;
  }

  /** Returns a GCEController using default application credentials. */
  public static GCEController newGCEController(
      String projectName,
      Map<String, Map<ClientParams, Integer>> types,
      int cores,
      ScheduledExecutorService executor) {
    try {
      HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
      JsonFactory jsonFactory = new JacksonFactory();
      GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
      if (credential.createScopedRequired()) {
        credential =
            credential.createScoped(
                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
      }
      Storage storage = new Storage.Builder(transport, jsonFactory, credential)
              .setApplicationName("Cloud Pub/Sub Loadtest Framework")
              .build();
      Compute compute = new Compute.Builder(transport, jsonFactory, credential)
                      .setApplicationName("Cloud Pub/Sub Loadtest Framework")
                      .build();
      Pubsub pubsub = new Pubsub.Builder(transport, jsonFactory, credential)
                      .setApplicationName("Cloud Pub/Sub Loadtest Framework")
                      .build();
      ArrayList<ResourceController> controllers = new ArrayList<>();
      ArrayList<String> subscriptions = new ArrayList<>();
      ArrayList<ComputeResourceController> computeControllers = new ArrayList<>();
      // Using atomic for effectively final not thread safety.
      AtomicBoolean hasJavaClient = new AtomicBoolean(false);
      types.forEach((zone, clientMap) -> {
        clientMap.forEach((params, count) -> {
          if (!params.clientType.isPublisher()) {
            subscriptions.add(params.subscription);
          }
          hasJavaClient.set(hasJavaClient.get() || params.clientType.language == Client.Language.JAVA);
        });
        GCEComputeResourceController computeController = new GCEComputeResourceController(
                projectName, zone, clientMap, cores, executor, compute
        );
        controllers.add(computeController);
        computeControllers.add(computeController);
      });
      controllers.add(new FirewallResourceController(projectName, executor, compute));
      controllers.add(new KafkaResourceController(Client.TOPIC, executor));
      controllers.add(new PubsubResourceController(projectName, Client.TOPIC, subscriptions, executor, pubsub));
      controllers.add(new StorageResourceController(projectName, Client.resourceDirectory, false, hasJavaClient.get(), executor, storage));
      return new GCEController(types, executor, controllers, computeControllers);
    } catch (Throwable t) {
      log.error("Unable to initialize GCE: ", t);
      return null;
    }
  }

  @Override
  public Map<String, Map<ClientParams, Integer>> getTypes() {
    return types;
  }
}
