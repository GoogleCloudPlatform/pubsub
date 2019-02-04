package com.google.pubsub.flic.controllers;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * A ResourceController which manages compute resources
 * */
public class GCEComputeResourceController extends ComputeResourceController {
    private static final String MACHINE_TYPE = "n1-standard-"; // standard machine prefix
    private static final String SOURCE_FAMILY =
            "projects/ubuntu-os-cloud/global/images/ubuntu-1604-xenial-v20171026a"; // Ubuntu 16.04 LTS

    protected static final Logger log = LoggerFactory.getLogger(GCEComputeResourceController.class);
    private final String project;
    private final String zone;
    private final Map<ClientParams, Integer> clients;
    private final Integer cores;
    private final ScheduledExecutorService executor;
    private final Compute compute;
    GCEComputeResourceController(String project, String zone, Map<ClientParams, Integer> clients, Integer cores, ScheduledExecutorService executor, Compute compute) {
        super(executor);
        this.project = project;
        this.zone = zone;
        this.clients = clients;
        this.cores = cores;
        this.executor = executor;
        this.compute = compute;
    }

    private String instanceGroupName(Client.ClientType type) {
        return "cps-loadtest-" + type + "-" + cores;
    }

    private void startInstance(Client.ClientType type, Integer count) throws Exception {
        int errors = 0;
        while (true) {
            try {
                // We first resize to 0 to delete any left running from an improperly cleaned up prior run.
                compute.instanceGroupManagers().resize(project, zone, instanceGroupName(type), 0).execute();
                compute.instanceGroupManagers().resize(project, zone, instanceGroupName(type), count).execute();
                return;
            } catch (GoogleJsonResponseException e) {
                if (errors > 10) {
                    throw e;
                }
                errors++;
                log.warn("InstanceGroupManager not yet ready, will try again.");
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public List<Client> startClients() throws Exception {
        // Start all instances
        List<ListenableFuture<Void>> futures = new ArrayList<>();
        clients.forEach((params, count) -> {
            SettableFuture<Void> future = SettableFuture.create();
            executor.execute(() -> {
                try {
                    startInstance(params.clientType, count);
                    future.set(null);
                } catch (Exception e) {
                    future.setException(e);
                }
            });
            futures.add(future);
        });
        Futures.allAsList(futures).get();

        // Create the clients
        List<SettableFuture<List<Client>>> clientFutures = new ArrayList<>();
        for (ClientParams params : clients.keySet()) {
            SettableFuture<List<Client>> future = SettableFuture.create();
            clientFutures.add(future);
            executor.execute(() -> {
                int numErrors = 0;
                while (true) {
                    try {
                        future.set(addInstanceGroupInfo(params));
                        return;
                    } catch (IOException e) {
                        numErrors++;
                        if (numErrors > 3) {
                            future.setException(new Exception("Failed to get instance information."));
                            return;
                        }
                        log.error("Transient error getting status for instance group, continuing", e);
                    }
                }
            });
        }

        return Futures.allAsList(clientFutures).get().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    private String instanceName(Client.ClientType type) {
        return "cps-loadtest-" + type.toString() + "-" + cores;
    }

    /**
     * Creates the default instance template for each type. Each type only changes the name and
     * startup script used.
     */
    private InstanceTemplate defaultInstanceTemplate(Client.ClientType type) {
        AccessConfig config = new AccessConfig();
        config.setType("ONE_TO_ONE_NAT");
        config.setName("External NAT");
        return new InstanceTemplate()
                .setName(instanceName(type))
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
                                                        + StorageResourceController.bucketName(project)
                                                        + "/"
                                                        + type.toString()
                                                        + "_startup_script.sh"),
                                        new Metadata.Items()
                                                .setKey("bucket")
                                                .setValue(StorageResourceController.bucketName(project)))))
                        .setServiceAccounts(Collections.singletonList(new ServiceAccount().setScopes(
                                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform")))));
    }

    /**
     * Creates the instance template and managed instance group for the given zone and type.
     */
    private void createManagedInstanceGroup(String zone, Client.ClientType type) throws Exception {
        // Create the Instance Template
        try {
            compute.instanceTemplates().insert(project,
                    defaultInstanceTemplate(type)).execute();
        } catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() != HttpStatus.SC_CONFLICT) {
                throw e;
            }
            log.info("Instance Template already exists for " + type + ", using existing template.");
        }

        // Create the Managed Instance Group
        while (true) {
            try {
                compute.instanceGroupManagers().insert(project, zone,
                        (new InstanceGroupManager()).setName(instanceGroupName(type))
                                .setInstanceTemplate("projects/" + project
                                        + "/global/instanceTemplates/cps-loadtest-" + type + "-" + cores)
                                .setTargetSize(0))
                        .execute();
                return;
            } catch (GoogleJsonResponseException e1) {
                if (e1.getStatusCode() == HttpStatus.SC_CONFLICT) {
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
     * For the given client type, return the relevant clients.
     */
    private List<Client> addInstanceGroupInfo(ClientParams params) throws IOException {
        ArrayList<Client> clients = new ArrayList<>();
        InstanceGroupManagersListManagedInstancesResponse response;
        do {
            response = compute.instanceGroupManagers().
                    listManagedInstances(project, zone, "cps-loadtest-"
                            + params.getClientType() + "-" + cores).execute();

            // If we are not instantiating any instances of this type, just return.
            if (response.getManagedInstances() == null) {
                return clients;
            }
        } while (!response.getManagedInstances().stream()
                .allMatch(i -> i.getCurrentAction().equals("NONE")));

        for (ManagedInstance managedInstance : response.getManagedInstances()) {
            String instanceName = managedInstance.getInstance()
                    .substring(managedInstance.getInstance().lastIndexOf('/') + 1);
            Instance instance = compute.instances().get(project, zone, instanceName).execute();
            clients.add(new Client(
                    params.getClientType(),
                    instance.getNetworkInterfaces().get(0).getAccessConfigs().get(0).getNatIP(),
                    project,
                    params.subscription,
                    executor));
        }
        return clients;
    }

    @Override
    protected void startAction() throws Exception {
        for (ClientParams params: clients.keySet()) {
            createManagedInstanceGroup(zone, params.clientType);
        }
    }

    @Override
    protected void stopAction() throws Exception {
        for (ClientParams params: clients.keySet()) {
            InstanceGroupManagersDeleteInstancesRequest request = new InstanceGroupManagersDeleteInstancesRequest();
            request.setInstances(ImmutableList.of(instanceName(params.clientType)));
            compute.instanceGroupManagers().deleteInstances(project, zone, instanceGroupName(params.clientType), request).execute();
        }
    }
}
