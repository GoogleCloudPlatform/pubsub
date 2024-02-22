/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.pubsub.flink.internal.source.reader;

import com.google.api.core.ApiFuture;
import com.google.common.base.Optional;
import com.google.pubsub.v1.PubsubMessage;

public interface NotifyingPullSubscriber {
  /** Returns a {@link ApiFuture} that will be completed when messages are available to pull */
  ApiFuture<Void> notifyDataAvailable();

  /** Pulls a message if one is available. */
  Optional<PubsubMessage> pullMessage() throws Throwable;

  /**
   * If there is an outstanding {@link ApiFuture} to notify when data is available, this method can
   * be used to interrupt the notification
   */
  void interruptNotify();

  void shutdown();
}
