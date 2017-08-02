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
import com.google.api.client.http.FileContent;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Base64;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.AttachedDisk;
import com.google.api.services.compute.model.AttachedDiskInitializeParams;
import com.google.api.services.compute.model.Firewall;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.InstanceGroupManager;
import com.google.api.services.compute.model.InstanceGroupManagersListManagedInstancesResponse;
import com.google.api.services.compute.model.InstanceProperties;
import com.google.api.services.compute.model.InstanceTemplate;
import com.google.api.services.compute.model.ManagedInstance;
import com.google.api.services.compute.model.Metadata;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.compute.model.ServiceAccount;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.pubsub.flic.controllers.Client.ClientType;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

/**
 * This is a subclass of {@link Controller} that controls load tests on Google Compute Engine.
 */
public class GCEController extends Controller {
  private static final String MACHINE_TYPE = "n1-standard-"; // standard machine prefix
  private static final String SOURCE_FAMILY =
      "projects/ubuntu-os-cloud/global/images/ubuntu-1604-xenial-v20160930"; // Ubuntu 16.04 LTS
  private static final int ALREADY_EXISTS = 409;
  private static final int NOT_FOUND = 404;
  public static String resourceDirectory = "target/classes/gce";
  private final Storage storage;
  private final Compute compute;
  private final String projectName;
  private final int cores;
  private final Map<String, Map<ClientParams, Integer>> types;

  /**
   * Instantiates the load test on Google Compute Engine.
   */
  private GCEController(String projectName, Map<String, Map<ClientParams, Integer>> types,
                        int cores, ScheduledExecutorService executor, Storage storage,
                        Compute compute, Pubsub pubsub) throws Throwable {
    super(executor);
    this.projectName = projectName;
    this.types = types;
    this.cores = cores;
    this.storage = storage;
    this.compute = compute;

    // For each unique type of CPS Publisher, create a Topic if it does not already exist, and then
    // delete and recreate any subscriptions attached to it so that we do not have backlog from
    // previous runs.
    List<SettableFuture<Void>> pubsubFutures = new ArrayList<>();
    types.values().forEach(paramsMap -> {
      paramsMap.keySet().stream().map(p -> p.getClientType())
          .distinct().filter(ClientType::isCpsPublisher).forEach(clientType -> {
        SettableFuture<Void> pubsubFuture = SettableFuture.create();
        pubsubFutures.add(pubsubFuture);
        executor.execute(() -> {
          String topic = Client.TOPIC_PREFIX + Client.getTopicSuffix(clientType);
          try {
            pubsub.projects().topics()
                .create("projects/" + projectName + "/topics/" + topic, new Topic()).execute();
          } catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() != ALREADY_EXISTS) {
              pubsubFuture.setException(e);
              return;
            }
            log.info("Topic already exists, reusing.");
          } catch (IOException e) {
            pubsubFuture.setException(e);
            return;
          }
          // Recreate each subscription attached to the topic.
          paramsMap.keySet().stream()
              .filter(p -> p.getClientType() == clientType.getSubscriberType())
              .map(p -> p.subscription).forEach(subscription -> {
            try {
              pubsub.projects().subscriptions().delete("projects/" + projectName
                  + "/subscriptions/" + subscription).execute();
            } catch (IOException e) {
              log.debug("Error deleting subscription, assuming it has not yet been created.", e);
            }
            try {
              pubsub.projects().subscriptions().create("projects/" + projectName
                  + "/subscriptions/" + subscription, new Subscription()
                  .setTopic("projects/" + projectName + "/topics/" + topic)
                  .setAckDeadlineSeconds(10)).execute();
            } catch (IOException e) {
              pubsubFuture.setException(e);
            }
          });
          pubsubFuture.set(null);
        });
      });
    });

    List<SettableFuture<Void>> kafkaFutures = new ArrayList<>();
    // If the Zookeeper host is provided, delete and recreate the topic
    // in order to eliminate performance issues from backlogs.
    if (!Client.zookeeperIpAddress.isEmpty()) {
      types.values().forEach(paramsMap -> {
        paramsMap.keySet().stream().map(p -> p.getClientType())
            .distinct().filter(ClientType::isKafkaPublisher).forEach(clientType -> {
          SettableFuture<Void> kafkaFuture = SettableFuture.create();
          kafkaFutures.add(kafkaFuture);
          executor.execute(() -> {
            String topic = Client.TOPIC_PREFIX + Client.getTopicSuffix(clientType);
            ZkClient zookeeperClient = new ZkClient(Client.zookeeperIpAddress, 15000, 10000,
                ZKStringSerializer$.MODULE$);
            ZkUtils zookeeperUtils = new ZkUtils(zookeeperClient,
                new ZkConnection(Client.zookeeperIpAddress), false);
            try {
              if (AdminUtils.topicExists(zookeeperUtils, topic)) {
                log.info("Deleting topic " + topic + ".");
                try {
                  AdminUtils.deleteTopic(zookeeperUtils, topic);
                } catch (ZkNodeExistsException e) {
                  log.info("Topic " + topic + " already marked for delete.");
                  kafkaFuture.setException(e);
                  return;
                }
              } else {
                log.info("Topic " + topic + " does not exist.");
              }
              while (AdminUtils.topicExists(zookeeperUtils, topic)) {
                // waiting for topic to delete before recreating
              }
              Properties topicConfig = new Properties();
              AdminUtils
                  .createTopic(zookeeperUtils, topic, Client.partitions, Client.replicationFactor,
                      AdminUtils.createTopic$default$5(),
                      AdminUtils.createTopic$default$6());
              log.info("Created topic " + topic + ".");
            } catch (Exception e) {
              kafkaFuture.setException(e);
              return;
            } finally {
              if (zookeeperClient != null) {
                zookeeperClient.close();
              }
            }
            kafkaFuture.set(null);
          });
        });
      });
    }

    try {
      createStorageBucket();
      // createFirewall();

      List<SettableFuture<Void>> filesRemaining = new ArrayList<>();
      Files.walk(Paths.get(resourceDirectory))
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
            createManagedInstanceGroup(zone, param.getClientType());
            createGroupFuture.set(null);
          } catch (Exception e) {
            createGroupFuture.setException(e);
          }
        });
      }));

      // Wait for files and instance groups to be created.
      Futures.allAsList(pubsubFutures).get();
      log.info("Pub/Sub actions completed.");
      Futures.allAsList(kafkaFutures).get();
      log.info("Kafka actions completed.");
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
            startInstances(zone, type.getClientType(), n);
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
      return new GCEController(
          projectName,
          types,
          cores,
          executor,
          new Storage.Builder(transport, jsonFactory, credential)
              .setApplicationName("Cloud Pub/Sub Loadtest Framework")
              .build(),
          new Compute.Builder(transport, jsonFactory, credential)
              .setApplicationName("Cloud Pub/Sub Loadtest Framework")
              .build(),
          new Pubsub.Builder(transport, jsonFactory, credential)
              .setApplicationName("Cloud Pub/Sub Loadtest Framework")
              .build());
    } catch (Throwable t) {
      log.error("Unable to initialize GCE: ", t);
      return null;
    }
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
                .resize(projectName, zone, "cps-loadtest-"
                    + param.getClientType() + "-" + cores, 0)
                .execute();
          } catch (IOException e) {
            log.error("Unable to resize Instance Group for " + param.getClientType() + ", please "
                + "manually ensure you do not have any running instances to avoid being billed.");
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
          .setName(projectName + "-cloud-pubsub-loadtest")).execute();
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
            (new InstanceGroupManager()).setName("cps-loadtest-" + type + "-" + cores)
                .setInstanceTemplate("projects/" + projectName
                    + "/global/instanceTemplates/cps-loadtest-" + type + "-" + cores)
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
            "cps-loadtest-" + type + "-" + cores, 0).execute();
        compute.instanceGroupManagers().resize(projectName, zone,
            "cps-loadtest-" + type + "-" + cores, n).execute();
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
      byte[] md5hash =
          Base64.decodeBase64(
              storage
                  .objects()
                  .get(projectName + "-cloud-pubsub-loadtest", filePath.getFileName().toString())
                  .execute()
                  .getMd5Hash());
      try (InputStream inputStream = Files.newInputStream(filePath, StandardOpenOption.READ)) {
        if (Arrays.equals(md5hash, DigestUtils.md5(inputStream))) {
          log.info("File " + filePath.getFileName() + " is current, reusing.");
          return;
        }
      }
      log.info("File " + filePath.getFileName() + " is out of date, uploading new version.");
      storage
          .objects()
          .delete(projectName + "-cloud-pubsub-loadtest", filePath.getFileName().toString())
          .execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() != NOT_FOUND) {
        throw e;
      }
    }

    storage
        .objects()
        .insert(
            projectName + "-cloud-pubsub-loadtest",
            null,
            new FileContent("application/octet-stream", filePath.toFile()))
        .setName(filePath.getFileName().toString())
        .execute();
    log.info("File " + filePath.getFileName() + " created.");
  }

  /**
   * For the given zone and client type, we add the instances created to the clients array, for the
   * base controller.
   */
  private void addInstanceGroupInfo(String zone, ClientParams params) throws IOException {
    InstanceGroupManagersListManagedInstancesResponse response;
    do {
      response = compute.instanceGroupManagers().
          listManagedInstances(projectName, zone, "cps-loadtest-"
              + params.getClientType() + "-" + cores).execute();

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
            params.getClientType(),
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
    AccessConfig config = new AccessConfig();
    config.setType("ONE_TO_ONE_NAT");
    config.setName("External NAT");
    return new InstanceTemplate()
        .setName("cps-loadtest-" + type + "-" + cores)
        .setProperties(new InstanceProperties()
            .setMachineType(MACHINE_TYPE + cores)
            .setDisks(Collections.singletonList(new AttachedDisk()
                .setBoot(true)
                .setAutoDelete(true)
                .setInitializeParams(new AttachedDiskInitializeParams()
                    .setSourceImage(SOURCE_FAMILY))))
            .setNetworkInterfaces(Collections.singletonList(new NetworkInterface()
                .setNetwork("global/networks/default")
                .setAccessConfigs(Collections.singletonList(config))))
            .setMetadata(new Metadata()
                .setItems(ImmutableList.of(
                    new Metadata.Items()
                        .setKey("startup-script-url")
                        .setValue("https://storage.googleapis.com/"
                            + projectName
                            + "-cloud-pubsub-loadtest/"
                            + type
                            + "_startup_script.sh"),
                    new Metadata.Items()
                        .setKey("bucket")
                        .setValue(projectName + "-cloud-pubsub-loadtest"))))
            .setServiceAccounts(Collections.singletonList(new ServiceAccount().setScopes(
                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform")))));
  }

  @Override
  public Map<String, Map<ClientParams, Integer>> getTypes() {
    return types;
  }
}
