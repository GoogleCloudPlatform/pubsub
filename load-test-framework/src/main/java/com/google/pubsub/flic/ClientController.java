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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientController {
  private static final Logger log = LoggerFactory.getLogger(ClientController.class.getName());
  private final Storage storage;
  private final Compute compute;
  private final Executor executor;
  private List<Client> clients;
  private List<GCEFile> files;

  public ClientController(List<Client> clients, Executor executor) throws IOException, GeneralSecurityException {
    this.clients = clients;
    this.executor = executor;
    HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
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

  // We probably want to
  //  a) Check file timestamps to avoid from having to deleting and recreating everything each go around.
  // Also some worries on this so far:
  //  1. If there are loadtests hanging around from a badly interrupted test before, we want to ensure we wait until
  //     they are all deleted so that it does not interfere with our test.
  //  2. We should probably wait until we get confirmation that they have already been successfully started.
  boolean initializeGCEProject(String projectName, String zone, List<String> types, int numberOfInstances) throws IOException, GeneralSecurityException {
    // here we can set up Storage / Metadata / InstanceTemplate
    AtomicBoolean success = new AtomicBoolean(true);
    Bucket loadtestBucket = storage.buckets().get("cloud-pubsub-loadtest").execute();
    if (loadtestBucket == null) {
      log.info("Bucket missing, creating a new bucket.");
      loadtestBucket = storage.buckets().insert(projectName, new Bucket()
          .setName("cloud-pubsub-loadtest")).execute();
    }
    if (loadtestBucket == null) {
      log.error("Unable to create a storage bucket!");
      return false;
    }

    CountDownLatch filesRemaining = new CountDownLatch(files.size());
    for (GCEFile file : files) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            StorageObject fileObject = storage.objects().get("cloud-pubsub-loadtest", file.getName()).execute();
            if (fileObject != null) {
              // Delete if it exists
              storage.objects().delete("cloud-pubsub-loadtest", file.getName()).execute();
            }
            fileObject = storage.objects().insert("cloud-pubsub-loadtests", null,
                new InputStreamContent("application/octet-stream", file.getInputStream()))
                .setName(file.getName()).execute();
            if (fileObject == null) {
              log.error("Unable to create a file!");
              success.set(false);
            }
          } catch (IOException e) {
            success.set(false);
          } finally {
            filesRemaining.countDown();
          }
        }
      });
    }
    CountDownLatch instancesRemaining = new CountDownLatch(types.size());
    for (String type : types) {
      executor.execute(
          new Runnable() {
            @Override
            public void run() {
              try {
                InstanceTemplate content = new InstanceTemplate();
                content.setName("cloud-pubsub-loadtests-instance-" + type);
                Metadata.Items startupScript = new Metadata.Items();
                startupScript.setKey("startup-script");
                startupScript.setValue("cloud-pubsub-loadtest/" + type + "_startup_script.sh");
                content.getProperties().getMetadata().getItems().add(startupScript);
                Operation response = compute.instanceTemplates().insert(projectName, content).execute();
                if (response == null) {
                  success.set(false);
                  return;
                }

                // we can leave it running if it exists. all we care about is tracking the new clients we'll create for this load test.
                if (compute.instanceGroupManagers().get(projectName, zone,
                    "cloud-pubsub-loadtest-framework-" + type).execute() == null) {
                  Operation result = compute.instanceGroupManagers().insert(projectName, zone,
                      (new InstanceGroupManager()).setName("cloud-pubsub-loadtest-framework-" + type)).execute();
                  if (result == null) {
                    success.set(false);
                    return;
                  }
                }
                Operation result = compute.instanceGroupManagers()
                    .setInstanceTemplate(projectName, zone, "cloud-pubsub-loadtest-framework-" + type,
                        (new InstanceGroupManagersSetInstanceTemplateRequest())
                            .setInstanceTemplate("cloud-pubsub-loadtests-instance-" + type)).execute();
                if (result == null) {
                  success.set(false);
                }
              } catch (IOException e) {
                success.set(false);
              } finally {
                instancesRemaining.countDown();
              }
            }
          }
      );
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
    for (String type : types) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            Operation operation = compute.instanceGroupManagers().resize(projectName, zone,
                "cloud-pubsub-loadtest-framework-" + type, 0).execute();
            if (operation == null) {
              success.set(false);
              return;
            }
            operation = compute.instanceGroupManagers().resize(projectName, zone,
                "cloud-pubsub-loadtest-framework-" + type, numberOfInstances).execute();
            success.set(operation != null);
          } catch (IOException e) {
            success.set(false);
          }
        }
      });
    }
    try {
      instanceGroupsToStart.await();
    } catch (InterruptedException e) {
      return false;
    }
    List<String> typesStillStarting = new ArrayList<String>(types);
    while (typesStillStarting.size() > 0) {
      CountDownLatch typesGettingInfo = new CountDownLatch(typesStillStarting.size());
      for (String type : typesStillStarting) {
        executor.execute(new Runnable() {
          @Override
          public void run() {
            try {
              InstanceGroupManagersListManagedInstancesResponse response = compute.instanceGroupManagers().
                  listManagedInstances(projectName, zone, "cloud-pubsub-loadtest-framework-" + type).execute();
              for (ManagedInstance instance : response.getManagedInstances()) {
                if (instance.getCurrentAction() != "NONE") {
                  typesGettingInfo.countDown();
                  return;
                }
              }
              synchronized (typesStillStarting) {
                typesStillStarting.remove(type);
              }
              typesGettingInfo.countDown();
            } catch (IOException e) {
              // ignore failure, try again, should mark this and set a max number of errors to allow here
            }
          }
        });
      }
      try {
        typesGettingInfo.await();
      } catch (InterruptedException e) {
        // log error, probably reflects a big problem??
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
    CPS_VENEER,
    CPS_GRPC,
    KAFKA
  }

  public class Client {
    private ClientType clientType;
    private String networkAddress;

    public Client(ClientType clientType, String networkAddress) {
      this.clientType = clientType;
      this.networkAddress = networkAddress;
    }

    public void start() {
    }
  }

  public class GCEFile {
    private String name;
    private InputStream inputStream;

    public String getName() {
      return name;
    }

    public InputStream getInputStream() {
      return inputStream;
    }

  }
}