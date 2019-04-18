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

package com.google.pubsub.flic.controllers;

import com.google.protobuf.Timestamp;
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

  /** Waits for clients to complete the load test. */
  void waitForClients() throws Throwable;

  /**
   * Gets the current start time.
   *
   * @return the start time
   */
  Timestamp getStartTime();

  /**
   * Gets the results for all available types.
   *
   * @return the map from type to result, every type running is a valid key
   */
  Map<ClientType, LatencyTracker> getClientLatencyTrackers();
}
