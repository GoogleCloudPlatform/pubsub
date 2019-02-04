package com.google.pubsub.flic.controllers;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

public abstract class ComputeResourceController extends ResourceController {
    ComputeResourceController(ScheduledExecutorService executor) {
        super(executor);
    }

    public abstract List<Client> startClients() throws Exception;
}
