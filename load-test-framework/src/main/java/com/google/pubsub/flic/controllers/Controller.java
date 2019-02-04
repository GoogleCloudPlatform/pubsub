package com.google.pubsub.flic.controllers;

import com.google.pubsub.flic.common.LatencyTracker;
import com.google.pubsub.flic.common.MessageTracker;

import java.util.Map;

public interface Controller {
    /**
     * Sends a LoadtestFramework.Start RPC to all clients to commence the load test. When this
     * function returns it is guaranteed that all clients have started.
     */
    void start(MessageTracker messageTracker);
    /**
     * Shuts down the given environment. When this function returns, each client is guaranteed to be
     * in process of being deleted, or else output directions on how to manually delete any potential
     * remaining instances if unable.
     */
    void stop();

    /**
     * @return the types map
     */
    Map<String, Map<ClientParams, Integer>> getTypes();

    /**
     * Waits for clients to complete the load test.
     */
    void waitForClients() throws Throwable;

    /**
     * Waits for publishers to complete the load test.
     */
    void waitForPublisherClients() throws Throwable;

    /**
     * Gets the results for all available types.
     *
     * @return the map from type to result, every type running is a valid key
     */
    Map<Client.ClientType, LatencyTracker> getClientLatencyTrackers();
}
