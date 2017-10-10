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

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.pubsub.flic.common.LatencyDistribution;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.pubsub.flic.common.LoadtestProto.MessageIdentifier;

import java.util.Arrays;
import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.stream.LongStream;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each subclass of Controller is responsible for instantiating and cleaning up a given environment.
 * When an environment is started, it adds {@link Client} objects to the clients array, which is
 * used to start the load test and collect results. This base class manages every
 * environment-agnostic part of this process.
 */
public abstract class Controller {
  protected static final Logger log = LoggerFactory.getLogger(Controller.class);
  public static String resourceDirectory = "target/classes/gce";
  protected final List<Client> clients = new ArrayList<>();
  protected final ScheduledExecutorService executor;

  /**
   * Creates the given environments and starts the virtual machines. When this function returns,
   * each client is guaranteed to have been connected and be network reachable, but is not started.
   * If an error occurred attempting to start the environment, the environment will be shut down,
   * and an Exception will be thrown. It is not guaranteed that we have completed shutting down when
   * this function returns, but it is guaranteed that we are in process.
   *
   * @param executor the executor that will be used to schedule all environment initialization tasks
   */
  public Controller(ScheduledExecutorService executor) {
    this.executor = executor;
  }

  /**
   * Shuts down the given environment. When this function returns, each client is guaranteed to be
   * in process of being deleted, or else output directions on how to manually delete any potential
   * remaining instances if unable.
   *
   * @param t the error that caused the shutdown, or null if shutting down successfully
   */
  public abstract void shutdown(Throwable t);

  /**
   * @return the types map
   */
  public abstract Map<String, Map<ClientParams, Integer>> getTypes();

  /**
   * Waits for clients to complete the load test.
   */
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

  /**
   * Waits for publishers to complete the load test.
   */
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

  /**
   * Gets the current statistics for the given type.
   *
   * @param type the client type to aggregate results for
   * @return the results from the load test up to this point
   */
  private LoadtestStats getStatsForClientType(Client.ClientType type) {
    LoadtestStats stats = new LoadtestStats();

    List<Client> clientsOfType = clients.stream()
        .filter(c -> c.getClientType() == type).collect(Collectors.toList());

    stats.runningSeconds =
        clientsOfType.stream().mapToLong(Client::getRunningSeconds).max().getAsLong()
            - Client.burnInDuration.getSeconds();

    clientsOfType.stream().map(Client::getBucketValues).forEach(bucketValues -> {
      for (int i = 0; i < LatencyDistribution.LATENCY_BUCKETS.length; i++) {
        stats.bucketValues[i] += bucketValues[i];
      }
    });

    stats.numOutOrderMsgs = new ArrayList<>();
    stats.outOrderMsgsPercent = new ArrayList<>();
    if (!type.isPublisher() && Client.orderTest) {
      clientsOfType.stream().map(Client::getMessagesReceived).forEach(list -> {
        MessageIdentifier[] msgs = new MessageIdentifier[list.size()];
        list.toArray(msgs);
        long outOfOrderMsgsCount = countOutOfOrder(msgs);
        stats.numOutOrderMsgs.add(outOfOrderMsgsCount);
        stats.outOrderMsgsPercent.add(
            Double.valueOf(String.format("%.2f", (outOfOrderMsgsCount * 100.0) / msgs.length)));
      });
    }

    return stats;
  }

  private Long countOutOfOrder(MessageIdentifier[] msgs) {
    Map<Long, Long> counters = new HashMap<>();
    Map<Long, Long> latestPublishTimestamps = new HashMap<>();

    Arrays.stream(msgs).forEach(msg -> {
      if (latestPublishTimestamps.get(msg.getPublisherClientId()) == null) {
        counters.put(msg.getPublisherClientId(), 0L);
        latestPublishTimestamps.put(msg.getPublisherClientId(), 0L);
      } else if (msg.getPublishTime() < latestPublishTimestamps.get(msg.getPublisherClientId())) {
        counters.put(msg.getPublisherClientId(), counters.get(msg.getPublisherClientId()) + 1);
      } else {
        latestPublishTimestamps.put(msg.getPublisherClientId(), msg.getPublishTime());
      }
    });

    return counters.values().stream().mapToLong(Number::longValue).sum();
  }

  /**
   * Gets the results for all available types.
   *
   * @return the map from type to result, every type running is a valid key
   */
  public Map<Client.ClientType, LoadtestStats> getStatsForAllClientTypes() {
    final Map<Client.ClientType, LoadtestStats> results = new HashMap<>();
    List<ListenableFuture<Void>> resultFutures = new ArrayList<>();
    getTypes().values().stream()
        .map(Map::keySet).flatMap(Set::stream)
        .map(ClientParams::getClientType).distinct()
        .forEach(type -> {
      SettableFuture<Void> resultFuture = SettableFuture.create();
      resultFutures.add(resultFuture);
      executor.submit(() -> {
        try {
          results.put(type, getStatsForClientType(type));
          resultFuture.set(null);
        } catch (Throwable t) {
          resultFuture.setException(t);
        }
      });
    });
    try {
      Futures.allAsList(resultFutures).get();
    } catch (ExecutionException | InterruptedException e) {
      log.error("Failed health check, will return results accumulated during test up to now.",
          e instanceof ExecutionException ? e.getCause() : e);
    }
    return results;
  }

  /**
   * Sends a LoadtestFramework.Start RPC to all clients to commence the load test. When this
   * function returns it is guaranteed that all clients have started.
   */
  public void startClients(MessageTracker messageTracker) {
    SettableFuture<Void> startFuture = SettableFuture.create();
    clients.forEach((client) -> executor.execute(() -> {
      try {
        client.start(messageTracker);
        startFuture.set(null);
      } catch (Throwable t) {
        startFuture.setException(t);
      }
    }));
    try {
      startFuture.get();
    } catch (ExecutionException e) {
      shutdown(e.getCause());
    } catch (InterruptedException e) {
      shutdown(e);
    }
  }

  /** The statistics that are exported by each load test client. */
  public static class LoadtestStats {
    public long runningSeconds;
    public List<Long> numOutOrderMsgs;
    public List<Double> outOrderMsgsPercent;
    public long[] bucketValues = new long[LatencyDistribution.LATENCY_BUCKETS.length];
    /** Returns the average QPS. */
    public double getQPS() {
      return (double) LongStream.of(bucketValues).sum() / (double) runningSeconds;
    }
    /** Returns the average throughput in MB/s. */
    public double getThroughput() {
      return getQPS() * Client.messageSize / 1000000.0;
    }
  }
}

