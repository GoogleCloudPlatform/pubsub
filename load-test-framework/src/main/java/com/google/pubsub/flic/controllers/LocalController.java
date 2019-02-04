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
import com.google.api.services.pubsub.Pubsub;
import com.google.common.collect.ImmutableList;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This is a subclass of {@link Controller} that controls local load tests.
 */
public class LocalController extends ControllerBase {
    private final Map<String, Map<ClientParams, Integer>> types;

    /**
     * Instantiates the load test on Google Compute Engine.
     */
    private LocalController(Map<String, Map<ClientParams, Integer>> types,
                            ScheduledExecutorService executor, List<ResourceController> controllers,
                            List<ComputeResourceController> computeControllers) {
        super(executor, controllers, computeControllers);
        this.types = types;
    }

    /** Returns a LocalController using default application credentials. */
    public static LocalController newLocalController(
            String projectName,
            Map<String, Map<ClientParams, Integer>> types,
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
            Pubsub pubsub = new Pubsub.Builder(transport, jsonFactory, credential)
                    .setApplicationName("Cloud Pub/Sub Loadtest Framework")
                    .build();
            ArrayList<ResourceController> controllers = new ArrayList<>();
            ArrayList<ComputeResourceController> computeControllers = new ArrayList<>();
            ArrayList<String> subscriptions = new ArrayList<>();
            types.forEach((zone, clientMap) -> {
                ComputeResourceController computeController = new LocalComputeResourceController(projectName, ImmutableList.copyOf(clientMap.keySet()), executor);
                controllers.add(computeController);
                computeControllers.add(computeController);
                clientMap.forEach((params, count) -> {
                    if (!params.clientType.isPublisher()) {
                        subscriptions.add(params.subscription);
                    }
                });
            });
            controllers.add(new PubsubResourceController(projectName, Client.TOPIC, subscriptions, executor, pubsub));
            return new LocalController(types, executor, controllers, computeControllers);
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

