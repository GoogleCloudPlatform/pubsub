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
import com.google.cloud.pubsub.*;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.pubsub.flic.controllers.Client.ClientType;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This is a subclass of {@link Controller} that controls load tests on Google Compute Engine.
 */
public class GCEController extends Controller {
  private static final String machineType = "n1-standard-4"; // quad core machines
  private static final String sourceFamily =
      "projects/ubuntu-os-cloud/global/images/ubuntu-1604-xenial-v20160930"; // Ubuntu 16.04 LTS
  private static final int ALREADY_EXISTS = 409;
  private static final int NOT_FOUND = 404;
  private final Storage storage;
  private final Compute compute;
  private final String projectName;
  private final Map<String, Map<ClientParams, Integer>> types;
  private int cpsPublisherCount;
  private int cpsSubscriberCount;
  private int kafkaPublisherCount;
  private int kafkaSubscriberCount;

  /**
   * Instantiates the load test on Google Compute Engine.
   */
  private GCEController(String projectName, Map<String, Map<ClientParams, Integer>> types,
                        ScheduledExecutorService executor, Storage storage,
                        Compute compute, PubSub pubSub) throws Throwable {
    super(executor);
    this.projectName = projectName;
    this.types = types;
    this.storage = storage;
    this.compute = compute;
    cpsPublisherCount = 0;
    cpsSubscriberCount = 0;
    kafkaPublisherCount = 0;
    kafkaSubscriberCount = 0;

    List<SettableFuture<Void>> pubsubFutures = new ArrayList<>();
    types.values().forEach((paramsMap) -> {
      // Iterate through map to aggregate subscriber and publisher counts
      paramsMap.forEach((param, count) -> {
        switch (param.clientType) {
          case CPS_GCLOUD_PUBLISHER:
            cpsPublisherCount += count;
            break;
          case CPS_GCLOUD_SUBSCRIBER:
            cpsSubscriberCount += count;
            break;
          case KAFKA_PUBLISHER:
            kafkaPublisherCount += count;
            break;
          case KAFKA_SUBSCRIBER:
            kafkaSubscriberCount += count;
            break;
        }
      });
      
      // For each unique type of CPS Publisher, create a Topic if it does not already exist, and then
      // delete and recreate any subscriptions attached to it so that we do not have backlog from
      // previous runs.
      paramsMap.keySet().stream().map(p -> p.clientType)
        .distinct().filter(ClientType::isCpsPublisher).forEach(clientType -> {
          SettableFuture<Void> pubsubFuture = SettableFuture.create();
          pubsubFutures.add(pubsubFuture);
          executor.execute(() -> {
            String topic = Client.topicPrefix + Client.getTopicSuffix(clientType);
            try {
              pubSub.create(TopicInfo.of(topic));
            } catch (PubSubException e) {
              if (!e.reason().equals("ALREADY_EXISTS")) {
                pubsubFuture.setException(e);
                return;
              }
              log.info("Topic already exists, reusing.");
            }
            // Recreate each subscription attached to the topic.
            paramsMap.keySet().stream()
                .filter(p -> p.clientType == clientType.getSubscriberType())
                .map(p -> p.subscription).forEach(subscription -> {
              pubSub.deleteSubscription(subscription);
              pubSub.create(SubscriptionInfo.of(topic, subscription));
            });
            pubsubFuture.set(null);
          });
        });
      });
    try {
      createStorageBucket();
      createFirewall();

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
      log.info("Pub/Sub actions completed.");
      Futures.allAsList(filesRemaining).get();
      log.info("File uploads completed.");
      Futures.allAsList(createGroupFutures).get();
      log.info("Instance group creation completed.");

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
          } catch (Exception e) {
            resizingFuture.setException(e);
          }
        });
      }));
      Futures.allAsList(resizingFutures).get();

      // We wait for all instances to finish starting, and get the external network address of each
      // newly created instance.
      List<SettableFuture<Void>> startFutures = new ArrayList<>();
      for (String zone : types.keySet()) {
        Map<ClientParams, Integer> paramsMap = types.get(zone);
        for (ClientParams type : paramsMap.keySet()) {
          SettableFuture<Void> startFuture = SettableFuture.create();
          startFutures.add(startFuture);
          executor.execute(() -> {
            int numErrors = 0;
            while (true) {
              try {
                addInstanceGroupInfo(zone, type);
                startFuture.set(null);
                return;
              } catch (IOException e) {
                numErrors++;
                if (numErrors > 3) {
                  startFuture.setException(new Exception("Failed to get instance information."));
                  return;
                }
                log.error("Transient error getting status for instance group, continuing", e);
              }
            }
          });
        }
      }

      Futures.allAsList(startFutures).get();
      log.info("Successfully started all instances.");
    } catch (ExecutionException e) {
      shutdown(e.getCause());
      throw e.getCause();
    } catch (Exception e) {
      shutdown(e);
      throw e;
    }
  }

  /**
   * Returns a GCEController using default application credentials.
   */
  public static GCEController newGCEController(
      String projectName,
      Map<String, Map<ClientParams, Integer>> types,
      ScheduledExecutorService executor) throws Throwable {
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

  /**
   * Shuts down all managed instance groups, or prints a log message so that you can go to Pantheon
   * in case of failure. This is idempotent and it is not an error to shutdown multiple times.
   */
  @Override
  public void shutdown(Throwable t) {
    if (t != null) {
      log.error("Shutting down: ", t);
    } else {
      log.info("Shutting down...");
    }
    // Attempt to cleanly close all running instances.
    types.forEach((zone, paramsCount) -> paramsCount.forEach((param, count) -> {
          try {
            compute.instanceGroupManagers()
                .resize(projectName, zone, "cloud-pubsub-loadtest-framework-" + param.clientType, 0)
                .execute();
          } catch (IOException e) {
            log.error("Unable to resize Instance Group for " + param.clientType + ", please " +
                "manually ensure you do not have any running instances to avoid being billed.");
          }
        })
    );
  }

  /**
   * Creates the storage bucket used by the load test.
   */
  private void createStorageBucket() throws IOException {
    try {
      storage.buckets().insert(projectName, new Bucket()
          .setName("cloud-pubsub-loadtest")).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() != ALREADY_EXISTS) {
        throw e;
      }
    }
  }

  /**
   * Adds a firewall rule to the default network so that we can connect to our clients externally.
   */
  private void createFirewall() throws IOException {
    Firewall firewallRule = new Firewall()
        .setName("cloud-loadtest-framework-firewall-rule")
        .setDescription("A firewall rule to allow the driver to coordinate load test instances.")
        .setAllowed(ImmutableList.of(
            new Firewall.Allowed()
                .setIPProtocol("tcp")
                .setPorts(Collections.singletonList("5000"))));
    try {
      compute.firewalls().insert(projectName, firewallRule).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() != ALREADY_EXISTS) {
        throw e;
      }
      compute.firewalls()
          .update(projectName, "cloud-loadtest-framework-firewall-rule", firewallRule).execute();
    }
  }

  /**
   * Creates the instance template and managed instance group for the given zone and type.
   */
  private void createManagedInstanceGroup(String zone, ClientType type) throws Exception {
    // Create the Instance Template
    try {
      compute.instanceTemplates().insert(projectName,
          defaultInstanceTemplate(type.toString())).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() != ALREADY_EXISTS) {
        throw e;
      }
      log.info("Instance Template already exists for " + type + ", using existing template.");
    }

    // Create the Managed Instance Group
    while (true) {
      try {
        compute.instanceGroupManagers().insert(projectName, zone,
            (new InstanceGroupManager()).setName("cloud-pubsub-loadtest-framework-" + type)
                .setInstanceTemplate("projects/" + projectName +
                    "/global/instanceTemplates/cloud-pubsub-loadtest-instance-" + type)
                .setTargetSize(0))
            .execute();
        return;
      } catch (GoogleJsonResponseException e1) {
        if (e1.getStatusCode() == ALREADY_EXISTS) {
          log.info("Instance Group already exists for " + type + ", using existing template.");
          return;
        }
        if (!e1.getDetails().getErrors().get(0).getReason().equals("resourceNotReady")) {
          throw e1;
        }
        log.debug("Instance template not ready for " + type + " trying again.");
        Thread.sleep(100);
      }
    }

  }

  /**
   * Re-sizes the instance groups to zero and then to the given size in order to ensure previous
   * runs do not interfere in case they were not cleaned up properly.
   */
  private void startInstances(String zone, ClientType type, Integer n) throws Exception {
    int errors = 0;
    while (true) {
      try {
        // We first resize to 0 to delete any left running from an improperly cleaned up prior run.
        compute.instanceGroupManagers().resize(projectName, zone,
            "cloud-pubsub-loadtest-framework-" + type, 0).execute();
        compute.instanceGroupManagers().resize(projectName, zone,
            "cloud-pubsub-loadtest-framework-" + type, n).execute();
        return;
      } catch (GoogleJsonResponseException e) {
        if (errors > 10) {
          throw e;
        }
        errors++;
        log.warn("InstanceGroupManager not yet ready, will try again.");
        Thread.sleep(100);
      }
    }
  }

  /**
   * Uploads a given file to Google Storage.
   */
  private void uploadFile(Path filePath) throws IOException {
    try {
      byte md5hash[] = Base64.decodeBase64(
          storage.objects().get("cloud-pubsub-loadtest", filePath.getFileName().toString())
              .execute().getMd5Hash()
      );
      try (InputStream inputStream = Files.newInputStream(filePath, StandardOpenOption.READ)) {
        if (Arrays.equals(md5hash, DigestUtils.md5(inputStream))) {
          log.info("File " + filePath.getFileName() + " is current, reusing.");
          return;
        }
      }
      log.info("File " + filePath.getFileName() + " is out of date, uploading new version.");
      storage.objects()
          .delete("cloud-pubsub-loadtest", filePath.getFileName().toString()).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() != NOT_FOUND) {
        throw e;
      }
    }
    try (InputStream inputStream = Files.newInputStream(filePath, StandardOpenOption.READ)) {
      storage.objects().insert("cloud-pubsub-loadtest", null,
          new InputStreamContent("application/octet-stream", inputStream))
          .setName(filePath.getFileName().toString()).execute();
      log.info("File " + filePath.getFileName() + " created.");
    }
  }

  /**
   * For the given zone and client type, we add the instances created to the clients array, for the
   * base controller.
   */
  private void addInstanceGroupInfo(String zone, ClientParams params) throws IOException {
    InstanceGroupManagersListManagedInstancesResponse response;
    do {
      response = compute.instanceGroupManagers().
          listManagedInstances(projectName, zone, "cloud-pubsub-loadtest-framework-" +
              params.clientType).execute();

      // If we are not instantiating any instances of this type, just return.
      if (response.getManagedInstances() == null) {
        return;
      }
    } while (!response.getManagedInstances().stream()
        .allMatch(i -> i.getCurrentAction().equals("NONE")));

    for (ManagedInstance managedInstance : response.getManagedInstances()) {
      String instanceName = managedInstance.getInstance()
          .substring(managedInstance.getInstance().lastIndexOf('/') + 1);
      Instance instance = compute.instances().get(projectName, zone, instanceName).execute();
      synchronized (this) {
        clients.add(new Client(
            params.clientType,
            instance.getNetworkInterfaces().get(0).getAccessConfigs().get(0).getNatIP(),
            projectName,
            params.subscription,
            executor));
      }
    }
  }

  /**
   * Creates the default instance template for each type. Each type only changes the name and
   * startup script used.
   */
  private InstanceTemplate defaultInstanceTemplate(String type) {
    return new InstanceTemplate()
        .setName("cloud-pubsub-loadtest-instance-" + type)
        .setProperties(new InstanceProperties()
            .setMachineType(machineType)
            .setDisks(Collections.singletonList(new AttachedDisk()
                .setBoot(true)
                .setAutoDelete(true)
                .setInitializeParams(new AttachedDiskInitializeParams()
                    .setSourceImage(sourceFamily))))
            .setNetworkInterfaces(Collections.singletonList(new NetworkInterface()
                .setNetwork("global/networks/default")
                .setAccessConfigs(Collections.singletonList(new AccessConfig()))))
            .setMetadata(new Metadata()
                .setItems(Collections.singletonList(new Metadata.Items()
                    .setKey("startup-script-url")
                    .setValue("https://storage.googleapis.com/cloud-pubsub-loadtest/" + type +
                        "_startup_script.sh"))))
            .setServiceAccounts(Collections.singletonList(new ServiceAccount().setScopes(
                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform")))));
  }

  /**
   * @return the cpsPublisherCount
   */
  public int getCpsPublisherCount() {
    return cpsPublisherCount;}

  /**
   * @return the cpsSubscriberCount
   */
  public int getCpsSubscriberCount() {
    return cpsSubscriberCount;}

  /**
   * @return the kafkaPublisherCount
   */
  public int getKafkaPublisherCount() {
    return kafkaPublisherCount;}

  /**
   * @return the kafkaSubscriberCount
   */
  public int getKafkaSubscriberCount() {
    return kafkaSubscriberCount;}
}