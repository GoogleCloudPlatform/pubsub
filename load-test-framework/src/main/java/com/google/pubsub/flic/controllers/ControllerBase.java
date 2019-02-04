package com.google.pubsub.flic.controllers;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.pubsub.flic.common.LatencyTracker;
import com.google.pubsub.flic.common.MessageTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public abstract class ControllerBase implements Controller {
    protected static final Logger log = LoggerFactory.getLogger(Controller.class);
    protected final List<Client> clients = new ArrayList<>();
    protected final ScheduledExecutorService executor;
    private final List<ResourceController> controllers;
    private final List<ComputeResourceController> computeControllers;
    private final Map<Client.ClientType, LatencyTracker> clientLatencyTrackers = new HashMap<>();

    /**
     * Creates the given environments and starts the virtual machines. When this function returns,
     * each client is guaranteed to have been connected and be network reachable, but is not started.
     * If an error occurred attempting to start the environment, the environment will be shut down,
     * and an Exception will be thrown. It is not guaranteed that we have completed shutting down when
     * this function returns, but it is guaranteed that we are in process.
     *
     * @param executor the executor that will be used to schedule all environment initialization tasks
     */
    public ControllerBase(ScheduledExecutorService executor, List<ResourceController> controllers,
                          List<ComputeResourceController> computeControllers) {
        this.executor = executor;
        this.controllers = controllers;
        this.computeControllers = computeControllers;
    }

    @Override
    public void waitForClients() throws Throwable {
        try {
            Futures.allAsList(clients.stream()
                    .map(Client::getDoneFuture)
                    .collect(Collectors.toList())
            ).get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Override
    public void waitForPublisherClients() throws Throwable {
        try {
            Futures.allAsList(clients.stream()
                    .filter(c -> c.getClientType().isPublisher())
                    .map(Client::getDoneFuture)
                    .collect(Collectors.toList())
            ).get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    private LatencyTracker getLatencyTrackerForType(Client.ClientType type) {
        if (!clientLatencyTrackers.containsKey(type)) {
            clientLatencyTrackers.put(type, new LatencyTracker());
        }
        return clientLatencyTrackers.get(type);
    }

    @Override
    public Map<Client.ClientType, LatencyTracker> getClientLatencyTrackers() {
        return clientLatencyTrackers;
    }

    @Override
    public void start(MessageTracker messageTracker) {
        // Start all ResourceControllers
        List<ListenableFuture<Void>> controllerFutures = new ArrayList<>();
        for (ResourceController controller : controllers) {
            controllerFutures.add(controller.start());
        }
        try {
            Futures.allAsList(controllerFutures).get();
        } catch (Exception e) {
            log.error("error starting controller: " + e);
            stop();
            return;
        }
        log.info("Started non-compute resources.");

        // Create all clients
        List<SettableFuture<List<Client>>> clientFutures = new ArrayList<>();
        computeControllers.forEach(controller -> {
            SettableFuture<List<Client>> future = SettableFuture.create();
            executor.execute(() -> {
                try {
                    future.set(controller.startClients());
                } catch (Exception e) {
                    future.setException(e);
                }
            });
            clientFutures.add(future);
        });
        try {
            clients.addAll(Futures.allAsList(clientFutures).get().stream().flatMap(Collection::stream).collect(Collectors.toList()));
        } catch (Exception e) {
            log.error("error starting compute: " + e);
            stop();
            return;
        }
        log.info("Started compute resources.");

        // Start all clients
        List<ListenableFuture<Void>> clientStartFutures = new ArrayList<>();
        for (Client client : clients) {
            SettableFuture<Void> future = SettableFuture.create();
            executor.execute(() -> {
                try {
                    client.start(messageTracker, getLatencyTrackerForType(client.getClientType()));
                    future.set(null);
                } catch (Throwable t) {
                    future.setException(t);
                }
            });
            clientStartFutures.add(future);
        }
        try {
            Futures.allAsList(clientStartFutures).get();
        } catch (Exception e) {
            log.error("error starting client: " + e);
            stop();
            return;
        }
        log.info("Started all clients.");
    }

    @Override
    public void stop() {
        ArrayList<ListenableFuture<Void>> futures = new ArrayList<>();
        for (ResourceController controller : controllers) {
            futures.add(controller.stop());
        }
        try {
            Futures.allAsList(futures).get();
        } catch (Exception e) {
            log.error("Failed to stop: " + e);
        }
    }
}
