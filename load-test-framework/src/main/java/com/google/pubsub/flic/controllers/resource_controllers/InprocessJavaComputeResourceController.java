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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.pubsub.clients.common.JavaLoadtestWorker;
import com.google.pubsub.clients.gcloud.CPSPublisherTask;
import com.google.pubsub.clients.gcloud.CPSSubscriberTask;
import com.google.pubsub.flic.controllers.Client;
import com.google.pubsub.flic.controllers.ClientParams;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InprocessJavaComputeResourceController extends ComputeResourceController {
  private static final Logger log = LoggerFactory.getLogger(LocalComputeResourceController.class);
  private final ClientParams params;
  private final ScheduledExecutorService executor;
  private JavaLoadtestWorker worker;

  public InprocessJavaComputeResourceController(
      ClientParams params, ScheduledExecutorService executor) {
    super(executor);
    this.params = params;
    this.executor = executor;
  }

  @Override
  protected void startAction() {}

  private int getPort() throws IOException {
    ServerSocket s = new ServerSocket(0);
    int port = s.getLocalPort();
    s.close();
    return port;
  }

  @Override
  public ListenableFuture<List<Client>> startClients() {
    SettableFuture<List<Client>> future = SettableFuture.create();
    executor.execute(
        () -> {
          try {
            Integer port = getPort();
            JavaLoadtestWorker.Options options = new JavaLoadtestWorker.Options();
            options.port = port;
            switch (params.getClientType().side) {
              case PUBLISHER:
                executor.execute(
                    () -> {
                      try {
                        worker =
                            new JavaLoadtestWorker(
                                options, new CPSPublisherTask.CPSPublisherFactory());
                      } catch (Exception e) {
                        throw new RuntimeException(e);
                      }
                    });
                future.set(ImmutableList.of(new Client("localhost", params, executor, port)));
                break;
              case SUBSCRIBER:
                executor.execute(
                    () -> {
                      try {
                        worker =
                            new JavaLoadtestWorker(
                                options, new CPSSubscriberTask.CPSSubscriberFactory());
                      } catch (Exception e) {
                        throw new RuntimeException(e);
                      }
                    });
                future.set(ImmutableList.of(new Client("localhost", params, executor, port)));
                break;
            }
          } catch (Exception e) {
            future.setException(e);
          }
        });
    return future;
  }

  @Override
  protected void stopAction() {
    worker = null;
  }
}
