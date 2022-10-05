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

package com.google.pubsub.flic.controllers.resource_controllers;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.*;
import com.google.pubsub.flic.controllers.Client;
import com.google.pubsub.flic.controllers.ClientParams;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A ResourceController which manages compute resource_controllers */
public class GCEComputeResourceController extends ComputeResourceController {
  private static final String MACHINE_TYPE = "n1-standard-"; // standard machine prefix
  private static final String SOURCE_FAMILY =
      "projects/ubuntu-os-cloud/global/images/ubuntu-2004-focal-v20220927"; // Ubuntu 20.04 LTS

  protected static final Logger log = LoggerFactory.getLogger(GCEComputeResourceController.class);
  private final String project;
  private final ClientParams params;
  private final Integer count;
  private final ScheduledExecutorService executor;
  private final Compute compute;

  public GCEComputeResourceController(
      String project,
      ClientParams params,
      Integer count,
      ScheduledExecutorService executor,
      Compute compute) {
    super(executor);
    this.project = project;
    this.params = params;
    this.count = count;
    this.executor = executor;
    this.compute = compute;
  }

  private String instanceName() {
    return "cps-loadtest-"
        + params.getClientType()
        + "-"
        + params.getTestParameters().numCoresPerWorker();
  }

  private void startInstances() throws Exception {
    int errors = 0;
    while (true) {
      try {
        // We first resize to 0 to delete any left running from an improperly cleaned up prior run.
        compute
            .instanceGroupManagers()
            .resize(project, params.getZone(), instanceName(), 0)
            .execute();
        compute
            .instanceGroupManagers()
            .resize(project, params.getZone(), instanceName(), count)
            .execute();
        log.info("InstanceGroupManager ready.");
        return;
      } catch (GoogleJsonResponseException e) {
        if (errors > 10) {
          throw e;
        }
        errors++;
        log.warn("InstanceGroupManager not yet ready, will try again.");
        Thread.sleep(5000);
      }
    }
  }

  @Override
  public ListenableFuture<List<Client>> startClients() {
    // Start instances
    SettableFuture<Void> startFuture = SettableFuture.create();
    executor.execute(
        () -> {
          try {
            startInstances();
            startFuture.set(null);
          } catch (Exception e) {
            startFuture.setException(e);
          }
        });

    SettableFuture<List<Client>> clientsFuture = SettableFuture.create();
    Futures.addCallback(
        startFuture,
        new FutureCallback<Void>() {
          @Override
          public void onSuccess(@Nullable Void aVoid) {
            int numErrors = 0;
            while (true) {
              try {
                clientsFuture.set(addInstanceGroupInfo());
                return;
              } catch (IOException e) {
                numErrors++;
                if (numErrors > 3) {
                  clientsFuture.setException(new Exception("Failed to get instance information."));
                  return;
                }
                log.error("Transient error getting status for instance group, continuing", e);
              } catch (InterruptedException e) {
                clientsFuture.setException(e);
              }
            }
          }

          @Override
          public void onFailure(Throwable throwable) {
            clientsFuture.setException(throwable);
          }
        },
        MoreExecutors.directExecutor());

    return clientsFuture;
  }

  /**
   * Creates the default instance template for each type. Each type only changes the name and
   * startup script used.
   */
  private InstanceTemplate defaultInstanceTemplate() {
    AccessConfig config = new AccessConfig();
    config.setType("ONE_TO_ONE_NAT");
    config.setName("External NAT");
    return new InstanceTemplate()
        .setName(instanceName())
        .setProperties(
            new InstanceProperties()
                .setMachineType(MACHINE_TYPE + params.getTestParameters().numCoresPerWorker())
                .setDisks(
                    Collections.singletonList(
                        new AttachedDisk()
                            .setBoot(true)
                            .setAutoDelete(true)
                            .setInitializeParams(
                                new AttachedDiskInitializeParams().setSourceImage(SOURCE_FAMILY))))
                .setNetworkInterfaces(
                    Collections.singletonList(
                        new NetworkInterface()
                            .setNetwork("global/networks/default")
                            .setAccessConfigs(Collections.singletonList(config))))
                .setMetadata(
                    new Metadata()
                        .setItems(
                            ImmutableList.of(
                                new Metadata.Items()
                                    .setKey("startup-script-url")
                                    .setValue(
                                        "https://storage.googleapis.com/"
                                            + StorageResourceController.bucketName(project)
                                            + "/"
                                            + params.getClientType()
                                            + "_startup_script.sh"),
                                new Metadata.Items()
                                    .setKey("bucket")
                                    .setValue(StorageResourceController.bucketName(project)))))
                .setServiceAccounts(
                    Collections.singletonList(
                        new ServiceAccount()
                            .setScopes(
                                Collections.singletonList(
                                    "https://www.googleapis.com/auth/cloud-platform")))));
  }

  /** Creates the instance template and managed instance group for the given zone and type. */
  private void createManagedInstanceGroup() throws Exception {
    // Create the Instance Template
    try {
      compute.instanceTemplates().insert(project, defaultInstanceTemplate()).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() != HttpStatus.SC_CONFLICT) {
        throw e;
      }
      log.info(
          "Instance Template already exists for "
              + params.getClientType()
              + ", using existing template.");
    }

    // Create the Managed Instance Group
    while (true) {
      try {
        compute
            .instanceGroupManagers()
            .insert(
                project,
                params.getZone(),
                (new InstanceGroupManager())
                    .setName(instanceName())
                    .setInstanceTemplate(
                        "projects/" + project + "/global/instanceTemplates/" + instanceName())
                    .setTargetSize(0))
            .execute();
        return;
      } catch (GoogleJsonResponseException e1) {
        if (e1.getStatusCode() == HttpStatus.SC_CONFLICT) {
          log.info(
              "Instance Group already exists for "
                  + params.getClientType()
                  + ", using existing template.");
          return;
        }
        if (!e1.getDetails().getErrors().get(0).getReason().equals("resourceNotReady")) {
          throw e1;
        }
        log.debug("Instance template not ready for " + params.getClientType() + " trying again.");
        Thread.sleep(100);
      }
    }
  }

  /** For the given client type, return the relevant clients. */
  private List<Client> addInstanceGroupInfo() throws IOException, InterruptedException {
    ArrayList<Client> clients = new ArrayList<>();
    InstanceGroupManagersListManagedInstancesResponse response;
    while (true) {
      try {
        response =
            compute
                .instanceGroupManagers()
                .listManagedInstances(project, params.getZone(), instanceName())
                .execute();
      } catch (GoogleJsonResponseException e) {
        log.warn("Unable to fetch response: ", e);
        continue;
      }

      if (response != null) {
        if (response.getManagedInstances() != null) {
          if (response.getManagedInstances().stream()
              .allMatch(i -> "RUNNING".equals(i.getInstanceStatus()))) {
            break;
          }
        }
      }

      log.warn("Instances not yet ready: " + (response == null ? "null" : response));
      Thread.sleep(10000);
    }

    for (ManagedInstance managedInstance : response.getManagedInstances()) {
      String instanceName =
          managedInstance
              .getInstance()
              .substring(managedInstance.getInstance().lastIndexOf('/') + 1);
      Instance instance =
          compute.instances().get(project, params.getZone(), instanceName).execute();
      clients.add(
          new Client(
              instance.getNetworkInterfaces().get(0).getAccessConfigs().get(0).getNatIP(),
              params,
              executor));
    }
    return clients;
  }

  @Override
  protected void startAction() throws Exception {
    createManagedInstanceGroup();
  }

  @Override
  protected void stopAction() throws Exception {
    log.info("Cleaning up compute resource_controllers.");
    compute.instanceGroupManagers().delete(project, params.getZone(), instanceName()).execute();
    boolean managerExists = true;
    while (managerExists) {
      try {
        compute.instanceGroupManagers().get(project, params.getZone(), instanceName()).execute();
      } catch (GoogleJsonResponseException e) {
        if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
          managerExists = false;
        } else {
          throw e;
        }
      }
      Thread.sleep(1000);
    }
    compute.instanceTemplates().delete(project, instanceName());
    log.info("Cleaned up compute resource_controllers.");
  }
}
