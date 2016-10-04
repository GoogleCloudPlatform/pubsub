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
import com.google.api.client.util.Base64;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.*;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import com.google.cloud.pubsub.SubscriptionInfo;
import com.google.cloud.pubsub.TopicInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.pubsub.flic.controllers.Client.ClientType;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class GCEController extends Controller {
  private static final Logger log = LoggerFactory.getLogger(GCEController.class.getName());
  private static final String machineType = "n1-standard-4"; // quad core machines
  private static final String sourceFamily = "projects/cloud-pubsub-load-tests/global/images/bring-down-the-world-image";
  private final Storage storage;
  private final Compute compute;
  private final PubSub pubSub;
  private final String projectName;
  private final Map<String, Map<ClientParams, Integer>> types;
  private boolean shutdown;

  private GCEController(String projectName, Map<String, Map<ClientParams, Integer>> types, Executor executor,
                        Storage storage, Compute compute, PubSub pubSub) {
    super(executor);
    this.shutdown = false;
    this.projectName = projectName;
    this.types = types;
    this.storage = storage;
    this.compute = compute;
    this.pubSub = pubSub;
  }

  public static GCEController newGCEController(String projectName,
                                               Map<String, Map<ClientParams, Integer>> types, Executor executor)
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
            .build(),
        PubSubOptions.builder().projectId(projectName).build().service());
  }

  @Override
  public synchronized void shutdown(Throwable t) {
    if (shutdown) {
      return;
    }
    shutdown = true;
    log.error("Shutting down: ", t);
    // Attempt to cleanly close all running instances.
    types.forEach((zone, paramsCount) -> paramsCount.forEach((param, count) -> {
          try {
            compute.instanceGroupManagers()
                .resize(projectName, zone, "cloud-pubsub-loadtest-framework-" + param.clientType, 0).execute();
          } catch (IOException e) {
            log.error("Unable to resize Instance Group for " + param.clientType +
                ", please manually ensure you do not have any running instances to avoid being billed.");
          }
        })
    );
  }

  @Override
  public void initialize() throws Throwable {
    synchronized (this) {
      if (shutdown) {
        throw new IOException("Already shutting down, cannot initialize.");
      }
    }
    List<SettableFuture<Void>> pubsubFutures = new ArrayList<>();
    types.values().forEach((paramsMap) -> paramsMap.keySet().stream().map((params) -> params.clientType)
        .distinct().forEach((clientType) -> {
          SettableFuture<Void> pubsubFuture = SettableFuture.create();
          pubsubFutures.add(pubsubFuture);
          executor.execute(() -> {
            // Delete each topic and subscription if it exists and create it new to avoid potential backlog from previous runs
            String topic = Client.topicPrefix + Client.getTopicSuffix(clientType);
            try {
              pubSub.create(TopicInfo.of(topic));
            } catch (Exception e) {
              log.info("Topic already exists, reusing.");
            }
            paramsMap.keySet().stream().filter((params) -> params.clientType == clientType && params.subscription != null)
                .map((params) -> params.subscription).forEach((subscription) -> {
              pubSub.deleteSubscription(subscription);
              pubSub.create(SubscriptionInfo.of(topic, subscription));
            });
            pubsubFuture.set(null);
          });
        }));
    try {
      createStorageBucket();

      Firewall firewallRule = new Firewall()
          .setName("cloud-loadtest-framework-firewall-rule")
          .setDescription("A firewall rule to allow the driver to coordinate load test instances.")
          .setAllowed(ImmutableList.of(
              new Firewall.Allowed()
                  .setIPProtocol("tcp")
                  .setPorts(Collections.singletonList("5000"))));
      try {
        compute.firewalls().get(projectName, "cloud-loadtest-framework-firewall-rule");
        compute.firewalls().update(projectName, "cloud-loadtest-framework-firewall-rule", firewallRule).execute();
      } catch (GoogleJsonResponseException e) {
        compute.firewalls().insert(projectName, firewallRule).execute();
      }
      List<SettableFuture<Void>> filesRemaining = new ArrayList<>();
      Files.walk(Paths.get("src/main/resources/gce"))
          .filter(Files::isRegularFile).forEach(filePath -> {
        SettableFuture<Void> fileRemaining = SettableFuture.create();
        filesRemaining.add(fileRemaining);
        executor.execute(() -> {
          try {
            uploadFile(filePath);
            fileRemaining.set(null);
          } catch (Exception e) {
            fileRemaining.setException(e);
          }
        });
      });
      List<SettableFuture<Void>> createGroupFutures = new ArrayList<>();
      types.forEach((zone, paramsMap) -> paramsMap.forEach((param, n) -> {
        SettableFuture<Void> createGroupFuture = SettableFuture.create();
        createGroupFutures.add(createGroupFuture);
        executor.execute(() -> {
          try {
            createManagedInstanceGroup(zone, param.clientType);
            createGroupFuture.set(null);
          } catch (Exception e) {
            createGroupFuture.setException(e);
          }
        });
      }));

      // Wait for files and instance groups to be created.
      Futures.allAsList(pubsubFutures).get();
      Futures.allAsList(filesRemaining).get();
      Futures.allAsList(createGroupFutures).get();

      // Everything is set up, let's start our instances
      log.info("Starting instances.");
      List<SettableFuture<Void>> resizingFutures = new ArrayList<>();
      types.forEach((zone, paramsMap) -> paramsMap.forEach((type, n) -> {
        SettableFuture<Void> resizingFuture = SettableFuture.create();
        resizingFutures.add(resizingFuture);
        executor.execute(() -> {
          try {
            startInstances(zone, type.clientType, n);
            resizingFuture.set(null);
          } catch (IOException e) {
            resizingFuture.setException(e);
          }
        });
      }));
      Futures.allAsList(resizingFutures).get();

      // We wait for all instances to finish starting, and get the external network address of each newly
      // created instance.
      waitForInstancesToStart();
      log.info("Successfully started all instances.");
    } catch (ExecutionException e) {
      shutdown(e.getCause());
      throw e.getCause();
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
    AtomicInteger maxErrors = new AtomicInteger(10);
    InterruptedException toThrow = new InterruptedException();
    types.forEach((zone, paramsMap) -> {
      List<ClientParams> typesStillStarting = new ArrayList<>(paramsMap.keySet());
      while (typesStillStarting.size() > 0) {
        CountDownLatch typesGettingInfo = new CountDownLatch(typesStillStarting.size());
        for (ClientParams type : typesStillStarting) {
          executor.execute(() -> {
            try {
              if (getInstanceGroupInfo(zone, type)) {
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
          log.error("Interrupted waiting for information about an instance group.");
          synchronized (toThrow) {
            if (toThrow.getCause() == null) {
              toThrow.initCause(e);
            }
          }
        }
      }
    });
    if (toThrow.getCause() != null) {
      throw toThrow;
    }
  }

  private void createManagedInstanceGroup(String zone, ClientType type) throws Exception {
    synchronized (this) {
      if (shutdown) {
        return;
      }
    }
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
      boolean created = false;
      while (!created) {
        try {
          compute.instanceGroupManagers().insert(projectName, zone,
              (new InstanceGroupManager()).setName("cloud-pubsub-loadtest-framework-" + type)
                  .setInstanceTemplate("projects/" + projectName +
                      "/global/instanceTemplates/cloud-pubsub-loadtest-instance-" + type)
                  .setTargetSize(0))
              .execute();
          created = true;
        } catch (GoogleJsonResponseException e1) {
          if (!e1.getDetails().getErrors().get(0).getReason().equals("resourceNotReady")) {
            throw e1;
          }
          log.info("Instance template not ready for " + type + " trying again.");
          Thread.sleep(100);
        }
      }
    }
  }

  private void startInstances(String zone, ClientType type, Integer n) throws IOException {
    synchronized (this) {
      if (shutdown) {
        return;
      }
    }
    // We first resize to 0 in case any were left running from an improperly cleaned up prior run.
    compute.instanceGroupManagers().resize(projectName, zone,
        "cloud-pubsub-loadtest-framework-" + type, 0).execute();
    compute.instanceGroupManagers().resize(projectName, zone,
        "cloud-pubsub-loadtest-framework-" + type, n).execute();
  }

  private void uploadFile(Path filePath) {
    synchronized (this) {
      if (shutdown) {
        return;
      }
    }
    try {
      byte md5hash[] = Base64.decodeBase64(
          storage.objects().get("cloud-pubsub-loadtest", filePath.getFileName().toString()).execute()
              .getMd5Hash()
      );
      try (InputStream inputStream = Files.newInputStream(filePath, StandardOpenOption.READ)) {
        if (Arrays.equals(md5hash, DigestUtils.md5(inputStream))) {
          log.info("File " + filePath.getFileName() + " is current, reusing.");
          return;
        }
      }
      log.info("File " + filePath.getFileName() + " is out of date, uploading new version.");
      return;
      //storage.objects().delete("cloud-pubsub-loadtest", filePath.getFileName().toString()).execute();
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

  private boolean getInstanceGroupInfo(String zone, ClientParams params) throws IOException {
    synchronized (this) {
      // If we are shutting down, we will report success so that the operation will not retry.
      if (shutdown) {
        return true;
      }
    }
    InstanceGroupManagersListManagedInstancesResponse response = compute.instanceGroupManagers().
        listManagedInstances(projectName, zone, "cloud-pubsub-loadtest-framework-" + params.clientType).execute();
    // if response is null, we are not instantiating any instances of this type
    if (response.getManagedInstances() == null) {
      return true;
    }
    for (ManagedInstance instance : response.getManagedInstances()) {
      if (!instance.getCurrentAction().equals("NONE")) {
        return false;
      }
    }
    for (ManagedInstance managedInstance : response.getManagedInstances()) {
      String instanceName = managedInstance.getInstance()
          .substring(managedInstance.getInstance().lastIndexOf('/') + 1);
      Instance instance = compute.instances().get(projectName, zone, instanceName).execute();
      clients.add(new Client(
          params.clientType,
          instance.getNetworkInterfaces().get(0).getAccessConfigs().get(0).getNatIP(),
          projectName,
          params.subscription));
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
    content.getProperties().setServiceAccounts(new ArrayList<>());
    content.getProperties().getServiceAccounts().add(new ServiceAccount().setScopes(
        Collections.singletonList("https://www.googleapis.com/auth/cloud-platform")));
    return content;
  }

  @Override
  public void startClients() {
    log.info("Starting clients.");
    super.startClients();
  }
}