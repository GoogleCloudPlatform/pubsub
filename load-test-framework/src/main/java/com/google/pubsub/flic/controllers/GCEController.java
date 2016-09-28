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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.*;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.pubsub.flic.clients.Client;
import com.google.pubsub.flic.clients.Client.ClientType;
import org.apache.commons.io.IOExceptionWithCause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.xml.Atom;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class GCEController extends Controller {
  private static final Logger log = LoggerFactory.getLogger(GCEController.class.getName());
  private final Storage storage;
  private final Compute compute;
  private final Executor executor;
  private List<Client> clients;
  private List<ClientType> types;
  private boolean shutdown;
  private final String machineType = "n1-standard-4"; // quad core machines
  private final String sourceFamily = "projects/debian-cloud/global/images/family/debian-8"; // latest Debian 8
  private final String projectName;
  private final String zone = "us-central1-a";
  private final int numberOfInstances = 10;

  public GCEController(String projectName, List<ClientType> types, Executor executor) throws IOException, GeneralSecurityException {
    this.executor = executor;
    this.shutdown = false;
    this.projectName = projectName;
    this.types = types;
    log.info("Starting GCEController");
    HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
    if (credential.createScopedRequired()) {
      credential =
          credential.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }
    this.storage = new Storage.Builder(transport, jsonFactory, credential)
        .setApplicationName("Cloud Pub/Sub Loadtest Framework")
        .build();
    this.compute = new Compute.Builder(transport, jsonFactory, credential)
        .setApplicationName("Cloud Pub/Sub Loadtest Framework")
        .build();
    // start the jobs
    // then send RPCs
    // then wait, print stats
    // close jobs
  }

  @Override
  synchronized void shutdown(Throwable t) {
    // close everything
    shutdown = true;
    log.error("Shutting down: ", t);
    // Attempt to cleanly close all running instances.
    clients.forEach(client -> {
      try {
        compute.instanceGroupManagers()
            .resize(projectName, zone, "cloud-pubsub-loadtest-framework-" + client.clientType(), 0).execute();
      } catch (IOException e) {
        // Ignore exceptions, clean close is best effort.
        log.error("Unable to resize Instance Group for " + client.clientType() +
            ", please manually ensure you do not have any running instances to avoid being billed.");
      }
    });
  }

  // We probably want to
  //  a) Check file timestamps to avoid from having to deleting and recreating everything each go around.
  // Also some worries on this so far:
  //  1. If there are loadtests hanging around from a badly interrupted test before, we want to ensure we wait until
  //     they are all deleted so that it does not interfere with our test.
  //  2. We should probably wait until we get confirmation that they have already been successfully started.
  @Override
  void initialize() throws IOException {
    synchronized (this) {
      if (shutdown) {
        return;
      }
    }
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

    Stream<Path> paths = Files.walk(Paths.get("src/main/resources/gce"));
    CountDownLatch filesRemaining = new CountDownLatch((int) paths.count());
    paths.filter(Files::isRegularFile).forEach(filePath -> executor.execute(() ->  {
      uploadFile(filePath);
      filesRemaining.countDown();
    }));
    paths.close();

    CountDownLatch instancesRemaining = new CountDownLatch(types.size());
    for (ClientType type : types) {
      executor.execute(() -> {
        try {
          try {
            compute.instanceTemplates().insert(projectName,
                defaultInstanceTemplate(type.toString())).execute();
          } catch (GoogleJsonResponseException e) {
            log.info("Instance Template already exists for " + type + ", using existing template.");
          }
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
        } finally {
          instancesRemaining.countDown();
        }
      });
    }
    try {
      filesRemaining.await();
      instancesRemaining.await();
    } catch (InterruptedException e) {
      log.error("Interrupted waiting for files and instance groups to be created.");
      shutdown(e);
      throw new IOException("Interrupted waiting for files and instance groups to be created.");
    }
    synchronized (this) {
      if (shutdown) {
        throw new IOException("Error uploading a file or creating instance templates or groups.");
      }
    }

    // Everything is set up, let's start our instances
    CountDownLatch instanceGroupsToStart = new CountDownLatch(types.size());
    for (ClientType type : types) {
      executor.execute(() -> {
          try {
            compute.instanceGroupManagers().resize(projectName, zone,
                "cloud-pubsub-loadtest-framework-" + type, 0).execute();
            compute.instanceGroupManagers().resize(projectName, zone,
                "cloud-pubsub-loadtest-framework-" + type, numberOfInstances).execute();
          } catch (IOException e) {
            shutdown(e);
          } finally {
            instanceGroupsToStart.countDown();
          }
      });
    }
    try {
      instanceGroupsToStart.await();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for instance groups to start.");
    }
    List<ClientType> typesStillStarting = new ArrayList<>(types);
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
      try {
        typesGettingInfo.await();
      } catch (InterruptedException e) {
        shutdown(e);
        throw new IOException("Interrupted waiting for files and instance groups to be created.");
      }
    }
    synchronized (this) {
      if (shutdown) {
        throw new IOException("Error starting and obtaining status of created instances.");
      }
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
      if (shutdown) {
        return false;
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

  // we need to know what kind of clients we started right? I mean there could and will be multiple instance groups:
  // one for each type of client. That actually is tough, because there will be multiple kinds of startup scripts.
  // We need some way of dynamically understanding what should be launched. I guess this will be supplied by cmd line
  // flags. But we still need to ensure that each ManagedInstanceGroup exists. Potentially we can cheat. So they have
  // to supply the type they want in flags like. --types=kafka,cps,veneer etc. and then we can name the scripts
  // kafka_startup_script.sh etc...
  void startClients() {
    clients.forEach(Client::start);
  }
}