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
package com.google.pubsub.flic;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.protobuf.Empty;
import com.google.pubsub.flic.common.Command.CommandRequest;
import com.google.pubsub.flic.common.LoadtestFrameworkGrpc;
import com.google.pubsub.flic.common.LoadtestFrameworkGrpc.LoadtestFrameworkStub;
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
import com.google.api.services.storage.model.StorageObject;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ClientController {
  private static final Logger log = LoggerFactory.getLogger(ClientController.class.getName());
  private final Storage storage;
  private final Compute compute;
  private final Executor executor;
  private List<Client> clients;
  private List<GCEFile> files;
  private boolean shutdown;
  private final String machineType = "n1-standard-4"; // quad core machines
  private final String sourceFamily = "projects/debian-cloud/global/images/family/debian-8"; // latest Debian 8

  public ClientController(String projectName, List<ClientType> clients, Executor executor) throws IOException, GeneralSecurityException {
    this.clients = new ArrayList<>(clients.size());
    this.executor = executor;
    this.shutdown = false;
    this.files = new ArrayList<>();
    log.info("Starting ClientController");
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
    loadFiles();
    initializeGCEProject(projectName, "us-central1-a", clients, 10);
    // start the jobs
    // then send RPCs
    // then wait, print stats
    // close jobs
  }

  private synchronized void shutdown(Throwable t) {
    // close everything
    shutdown = true;
    log.error("Shutting down: ", t);
    files.forEach(gcefile -> {
      try {
        gcefile.getInputStream().close();
      } catch (IOException e) {
        // we ignore failure on close
      }
    });
  }

  synchronized void loadFiles() {
    if (shutdown) {
      return;
    }
    // read all files in directories, and append to files
    try (Stream<Path> paths = Files.walk(Paths.get("resources/gce"))) {
      paths.forEach(filePath -> {
        if (Files.isRegularFile(filePath)) {
          try {
            files.add(new GCEFile(filePath.getFileName().toString(), Files.newInputStream(filePath, StandardOpenOption.READ)));
          } catch (IOException e) {
            shutdown(e);
          }
        }
      });
    } catch (IOException e) {
      //shutdown(e);
    }
  }
  // We probably want to
  //  a) Check file timestamps to avoid from having to deleting and recreating everything each go around.
  // Also some worries on this so far:
  //  1. If there are loadtests hanging around from a badly interrupted test before, we want to ensure we wait until
  //     they are all deleted so that it does not interfere with our test.
  //  2. We should probably wait until we get confirmation that they have already been successfully started.
  boolean initializeGCEProject(String projectName, String zone, List<ClientType> types, int numberOfInstances) throws IOException {
    // here we can set up Storage / Metadata / InstanceTemplate
    synchronized (this) {
      if (shutdown) {
        return false;
      }
    }
    AtomicBoolean success = new AtomicBoolean(true);
    try {
      storage.buckets().get("cloud-pubsub-loadtest").execute();
    } catch (GoogleJsonResponseException e) {
      log.info("Bucket missing, creating a new bucket.");
      try {
        storage.buckets().insert(projectName, new Bucket()
            .setName("cloud-pubsub-loadtest")).execute();
      } catch (GoogleJsonResponseException e1) {
        shutdown(e1);
        return false;
      }
    }

    CountDownLatch filesRemaining = new CountDownLatch(files.size());
    for (GCEFile file : files) {
      executor.execute(() -> {
          try {
            storage.objects().get("cloud-pubsub-loadtest", file.getName()).execute();
            log.info("File already exists, will delete and recreate it.");
            storage.objects().delete("cloud-pubsub-loadtest", file.getName()).execute();
          } catch (Exception e) {
            log.info("File does not already exist.");
          }
          try {
            storage.objects().insert("cloud-pubsub-loadtests", null,
                new InputStreamContent("application/octet-stream", file.getInputStream()))
                .setName(file.getName()).execute();
          } catch (IOException e) {
            shutdown(e);
            success.set(false);
          } finally {
            filesRemaining.countDown();
          }
      });
    }
    CountDownLatch instancesRemaining = new CountDownLatch(types.size());
    for (ClientType type : types) {
      executor.execute(() -> {
        try {
          try {
            InstanceTemplate content = new InstanceTemplate();
            content.setName("cloud-pubsub-loadtests-instance-" + type);
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
            metadata.setKey("startup-script");
            metadata.setValue("cloud-pubsub-loadtest/" + type + "_startup_script.sh");
            content.getProperties().getMetadata().getItems().add(metadata);
            compute.instanceTemplates().insert(projectName, content).execute();
          } catch (GoogleJsonResponseException e) {
            // we can safely ignore this, since we will always use the same
            // parameters.
          }
          // We can leave it running if it exists.
          // All we care about is tracking the new clients we'll create for this load test.
          try {
            compute.instanceGroupManagers().get(projectName, zone,
                "cloud-pubsub-loadtest-framework-" + type).execute();
          } catch (GoogleJsonResponseException e) {
            compute.instanceGroupManagers().insert(projectName, zone,
                (new InstanceGroupManager()).setName("cloud-pubsub-loadtest-framework-" + type)
                    .setInstanceTemplate("projects/" + projectName +
                        "/global/instanceTemplates/cloud-pubsub-loadtests-instance-" + type)
                    .setTargetSize(0))
                .execute();
          }
        } catch (IOException e) {
          shutdown(e);
          success.set(false);
        } finally {
          instancesRemaining.countDown();
        }
      });
    }
    try {
      filesRemaining.await();
      instancesRemaining.await();
    } catch (InterruptedException e) {
      log.error("Interrupted waiting for worker tasks to complete.");
      return false;
    }
    if (!success.get()) {
      return false;
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
            success.set(false);
          } finally {
            instanceGroupsToStart.countDown();
          }
      });
    }
    try {
      instanceGroupsToStart.await();
    } catch (InterruptedException e) {
      return false;
    }
    List<ClientType> typesStillStarting = new ArrayList<>(types);
    AtomicInteger maxErrors = new AtomicInteger(10);
    while (typesStillStarting.size() > 0 && success.get()) {
      CountDownLatch typesGettingInfo = new CountDownLatch(typesStillStarting.size());
      for (ClientType type : typesStillStarting) {
        executor.execute(() -> {
            try {
              InstanceGroupManagersListManagedInstancesResponse response = compute.instanceGroupManagers().
                  listManagedInstances(projectName, zone, "cloud-pubsub-loadtest-framework-" + type).execute();
              for (ManagedInstance instance : response.getManagedInstances()) {
                if (!instance.getCurrentAction().equals("NONE")) {
                  typesGettingInfo.countDown();
                  return;
                }
              }
              for (ManagedInstance managedInstance : response.getManagedInstances()) {
                String instanceName = managedInstance.getInstance()
                    .substring(managedInstance.getInstance().lastIndexOf('/') + 1);
                log.info(instanceName);
                Instance instance = compute.instances().get(projectName, zone, instanceName).execute();
                String ip = null;
                for (NetworkInterface networkInterface : instance.getNetworkInterfaces()) {
                  for (AccessConfig accessConfig : networkInterface.getAccessConfigs()) {
                    if (accessConfig.getNatIP() != null) {
                      ip = accessConfig.getNatIP();
                      break;
                    }
                  }
                  if (ip != null) {
                    break;
                  }
                }
                clients.add(new Client(type, ip));
              }
              synchronized (typesStillStarting) {
                typesStillStarting.remove(type);
              }
            } catch (IOException e) {
              if (maxErrors.decrementAndGet() == 0) {
                log.error("Having trouble connecting to GCE, shutting down.");
                shutdown(e);
                success.set(false);
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
        return false;
      }
    }
    return success.get();
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

  public enum ClientType {
    CPS_VENEER("veneer"),
    CPS_GRPC("grpc"),
    KAFKA("kafka");

    private final String text;

    private ClientType(final String text) {
      this.text = text;
    }

    @Override
    public String toString() {
      return text;
    }
  }

  public enum ClientStatus {
    NONE,
    RUNNING,
    STOPPING,
  }

  public class Client {
    private ClientType clientType;
    private String networkAddress;
    private ClientStatus clientStatus;

    public Client(ClientType clientType, String networkAddress) {
      this.clientType = clientType;
      this.networkAddress = networkAddress;
      this.clientStatus = ClientStatus.NONE;
    }

    public void start() {
      // Send a gRPC call to start the server
      // select port? 5000 always a default? set this somewhere
      ManagedChannel channel = ManagedChannelBuilder.forAddress(networkAddress, 5000).usePlaintext(true).build();

      LoadtestFrameworkStub stub = LoadtestFrameworkGrpc.newStub(channel);
      CommandRequest request = CommandRequest.newBuilder().build();
      stub.startClient(request, new StreamObserver<Empty>() {
        @Override
        public void onNext(Empty empty) {
          log.info("Successfully started client [" + networkAddress + "]");
          clientStatus = ClientStatus.RUNNING;
        }

        @Override
        public void onError(Throwable throwable) {
          shutdown(throwable);
        }

        @Override
        public void onCompleted() {}
      });

    }
  }

  class GCEFile {
    private String name;
    private InputStream inputStream;

    public GCEFile(String name, InputStream inputStream) {
      this.name = name;
      this.inputStream = inputStream;
    }

    public String getName() {
      return name;
    }

    public InputStream getInputStream() {
      return inputStream;
    }
  }
}