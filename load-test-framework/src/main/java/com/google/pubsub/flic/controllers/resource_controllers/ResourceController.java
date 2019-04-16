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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A ResourceController creates a resource when start is called and cleans it up when stop is
 * called.
 *
 * <p>These operations are complete when their future returns.
 */
public abstract class ResourceController {
  private final ScheduledExecutorService executor;

  protected ResourceController(ScheduledExecutorService executor) {
    this.executor = executor;
  }

  public ListenableFuture<Void> start() {
    SettableFuture<Void> future = SettableFuture.create();
    executor.execute(
        () -> {
          try {
            startAction();
          } catch (Exception e) {
            future.setException(e);
            return;
          }
          future.set(null);
        });
    return future;
  }

  public ListenableFuture<Void> stop() {
    SettableFuture<Void> future = SettableFuture.create();
    executor.execute(
        () -> {
          try {
            stopAction();
          } catch (Exception e) {
            future.setException(e);
            return;
          }
          future.set(null);
        });
    return future;
  }

  protected abstract void startAction() throws Exception;

  protected abstract void stopAction() throws Exception;
}
