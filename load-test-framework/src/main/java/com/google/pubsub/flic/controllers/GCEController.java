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
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.*;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.common.base.Preconditions;
import com.google.pubsub.flic.controllers.Client.ClientType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class GCEController extends Controller {
  private static final Logger log = LoggerFactory.getLogger(GCEController.class.getName());
  private final Storage storage;
  private final Compute compute;
  private final Executor executor;
  private final String machineType = "n1-standard-4"; // quad core machines
  private final String sourceFamily = "projects/debian-cloud/global/images/family/debian-8"; // latest Debian 8
  private final String projectName;
  private final String zone = "us-central1-a";
  private Map<ClientType, Integer> types;
  private boolean shutdown;

  private GCEController(String projectName, Map<ClientType, Integer> types, Executor executor,
                        Storage storage, Compute compute) {
    this.executor = executor;
    this.shutdown = false;
    this.projectName = projectName;
    this.types = types;
    this.storage = storage;
    this.compute = compute;
  }

  public static GCEController newGCEController(String projectName, Map<ClientType, Integer> types, Executor executor)
      throws IOException, GeneralSecurityException {
    HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
    if (credential.createScopedRequired()) {
      credential =
          credential.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }
    return new GCEController(projectName, types, executor,
        new Storage.Builder(transport, jsonFactory, credential)
            .setApplicationName("Cloud Pub/Sub Loadtest Framework")
            .build(),
        new Compute.Builder(transport, jsonFactory, credential)
            .setApplicationName("Cloud Pub/Sub Loadtest Framework")
            .build());
  }

  @Override
  synchronized void shutdown(Throwable t) {
    if (shutdown) {
      return;
    }
    shutdown = true;
    log.error("Shutting down: ", t);
    // Attempt to cleanly close all running instances.
    clients.forEach(client -> {
      try {
        compute.instanceGroupManagers()
            .resize(projectName, zone, "cloud-pubsub-loadtest-framework-" + client.clientType(), 0).execute();
      } catch (IOException e) {
        log.error("Unable to resize Instance Group for " + client.clientType() +
            ", please manually ensure you do not have any running instances to avoid being billed.");
      }
    });
  }

  @Override
  void initialize() throws IOException, InterruptedException {
    synchronized (this) {
      if (shutdown) {
        throw new IOException("Already shutting down, cannot initialize.");
      }
    }
    try {
      createStorageBucket();

      CountDownLatch filesRemaining;
      try (Stream<Path> paths = Files.walk(Paths.get("src/main/resources/gce"))) {
        Preconditions.checkArgument(paths.count() < Integer.MAX_VALUE);
        filesRemaining = new CountDownLatch((int) paths.count());
        paths.filter(Files::isRegularFile).forEach(filePath -> executor.execute(() -> {
          uploadFile(filePath);
          filesRemaining.countDown();
        }));
      }

      CountDownLatch instancesRemaining = new CountDownLatch(types.size());
      types.forEach((type, n) -> executor.execute(() -> {
            createManagedInstanceGroup(type);
            instancesRemaining.countDown();
          }
      ));

      // Wait for files and instance groups to be created.
      filesRemaining.await();
      instancesRemaining.await();

      // Everything is set up, let's start our instances
      types.forEach((type, n) -> executor.execute(() -> startInstances(type, n)));

      // We wait for all instances to finish starting, and get the external network address of each newly
      // created instance.
      waitForInstancesToStart();
    } catch (Exception e) {
      shutdown(e);
      throw e;
    }
  }

  private void createStorageBucket() {
    synchronized (this) {
      if (shutdown) {
        return;
      }
    }
    try {
      try {
        storage.buckets().get("cloud-pubsub-loadtest").execute();
      } catch (GoogleJsonResponseException e) {
        log.info("Bucket missing, creating a new bucket.");
        try {
          storage.buckets().insert(projectName, new Bucket()
              .setName("cloud-pubsub-loadtest")).execute();
        } catch (GoogleJsonResponseException e1) {
          shutdown(e1);
          throw e1;
        }
      }
    } catch (IOException e) {
      shutdown(e);
    }
  }

  private void waitForInstancesToStart() throws InterruptedException {
    synchronized (this) {
      if (shutdown) {
        return;
      }
    }
    List<ClientType> typesStillStarting = new ArrayList<>(types.keySet());
    AtomicInteger maxErrors = new AtomicInteger(10);
    while (typesStillStarting.size() > 0) {
      CountDownLatch typesGettingInfo = new CountDownLatch(typesStillStarting.size());
      for (ClientType type : typesStillStarting) {
        executor.execute(() -> {
          try {
            if (getInstanceGroupInfo(type)) {
              typesStillStarting.remove(type);
            }
          } catch (IOException e) {
            if (maxErrors.decrementAndGet() == 0) {
              log.error("Having trouble connecting to GCE, shutting down.");
              shutdown(e);
            } else {
              log.error("Transient error getting status for instance group, continuing", e);
            }
          } finally {
            typesGettingInfo.countDown();
          }
        });
      }
      typesGettingInfo.await();
    }
  }

  private void createManagedInstanceGroup(ClientType type) {
    synchronized (this) {
      if (shutdown) {
        return;
      }
    }
    try {
      // Create the Instance Template
      try {
        compute.instanceTemplates().insert(projectName,
            defaultInstanceTemplate(type.toString())).execute();
      } catch (GoogleJsonResponseException e) {
        log.info("Instance Template already exists for " + type + ", using existing template.");
      }
      // Create the Managed Instance Group
      try {
        compute.instanceGroupManagers().get(projectName, zone,
            "cloud-pubsub-loadtest-framework-" + type).execute();
        log.info("Managed Instance Group already exists for " + type + ", using existing group.");
      } catch (GoogleJsonResponseException e) {
        log.info("Creating new Managed Instance Group for " + type);
        compute.instanceGroupManagers().insert(projectName, zone,
            (new InstanceGroupManager()).setName("cloud-pubsub-loadtest-framework-" + type)
                .setInstanceTemplate("projects/" + projectName +
                    "/global/instanceTemplates/cloud-pubsub-loadtest-instance-" + type)
                .setTargetSize(0))
            .execute();
      }
    } catch (IOException e) {
      shutdown(e);
    }
  }

  private void startInstances(ClientType type, Integer n) {
    synchronized (this) {
      if (shutdown) {
        return;
      }
    }
    try {
      // We first resize to 0 in case any were left running from an improperly cleaned up prior run.
      compute.instanceGroupManagers().resize(projectName, zone,
          "cloud-pubsub-loadtest-framework-" + type, 0).execute();
      compute.instanceGroupManagers().resize(projectName, zone,
          "cloud-pubsub-loadtest-framework-" + type, n).execute();
    } catch (IOException e) {
      shutdown(e);
    }
  }

  private void uploadFile(Path filePath) {
    synchronized (this) {
      if (shutdown) {
        return;
      }
    }
    try {
      storage.objects().get("cloud-pubsub-loadtest", filePath.getFileName().toString()).execute();
      log.info("File " + filePath.getFileName() + " already exists, will delete and recreate it.");
      storage.objects().delete("cloud-pubsub-loadtest", filePath.getFileName().toString()).execute();
    } catch (IOException e) {
      log.info("File " + filePath.getFileName() + " does not already exist.");
    }
    try (InputStream inputStream = Files.newInputStream(filePath, StandardOpenOption.READ)) {
      storage.objects().insert("cloud-pubsub-loadtest", null,
          new InputStreamContent("application/octet-stream", inputStream))
          .setName(filePath.getFileName().toString()).execute();
      log.info("File " + filePath.getFileName() + " created.");
    } catch (IOException e) {
      shutdown(e);
    }
  }

  private boolean getInstanceGroupInfo(ClientType type) throws IOException {
    synchronized (this) {
      // If we are shutting down, we will report success so that the operation will not retry.
      if (shutdown) {
        return true;
      }
    }
    InstanceGroupManagersListManagedInstancesResponse response = compute.instanceGroupManagers().
        listManagedInstances(projectName, zone, "cloud-pubsub-loadtest-framework-" + type).execute();
    for (ManagedInstance instance : response.getManagedInstances()) {
      if (!instance.getCurrentAction().equals("NONE")) {
        return false;
      }
    }
    for (ManagedInstance managedInstance : response.getManagedInstances()) {
      String instanceName = managedInstance.getInstance()
          .substring(managedInstance.getInstance().lastIndexOf('/') + 1);
      Instance instance = compute.instances().get(projectName, zone, instanceName).execute();
      clients.add(new Client(type, instance.getNetworkInterfaces().get(0)
          .getAccessConfigs().get(0).getNatIP()));
    }
    return true;
  }

  private InstanceTemplate defaultInstanceTemplate(String type) {
    InstanceTemplate content = new InstanceTemplate();
    content.setName("cloud-pubsub-loadtest-instance-" + type);
    content.setProperties(new InstanceProperties());
    content.getProperties().setMachineType(machineType);
    List<AttachedDisk> disks = new ArrayList<>();
    disks.add(new AttachedDisk());
    disks.get(0).setInitializeParams(new AttachedDiskInitializeParams());
    disks.get(0).getInitializeParams().setSourceImage(sourceFamily);
    disks.get(0).setBoot(true);
    content.getProperties().setDisks(disks);
    List<NetworkInterface> networkInterfaces = new ArrayList<>();
    networkInterfaces.add(new NetworkInterface());
    networkInterfaces.get(0).setNetwork("global/networks/default");
    networkInterfaces.get(0).setAccessConfigs(new ArrayList<>());
    networkInterfaces.get(0).getAccessConfigs().add(new AccessConfig());
    content.getProperties().setNetworkInterfaces(networkInterfaces);
    content.getProperties().setMetadata(new Metadata());
    content.getProperties().getMetadata().setItems(new ArrayList<>());
    Metadata.Items metadata = new Metadata.Items();
    metadata.setKey("startup-script-url");
    metadata.setValue("https://storage.googleapis.com/cloud-pubsub-loadtest/" + type + "_startup_script.sh");
    content.getProperties().getMetadata().getItems().add(metadata);
    return content;
  }
}