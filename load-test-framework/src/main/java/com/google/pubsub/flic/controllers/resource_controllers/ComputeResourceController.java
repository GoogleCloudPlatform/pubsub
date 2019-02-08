package com.google.pubsub.flic.controllers.resource_controllers;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.flic.controllers.Client;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

public abstract class ComputeResourceController extends ResourceController {
    ComputeResourceController(ScheduledExecutorService executor) {
        super(executor);
    }

    public abstract ListenableFuture<List<Client>> startClients();
}
