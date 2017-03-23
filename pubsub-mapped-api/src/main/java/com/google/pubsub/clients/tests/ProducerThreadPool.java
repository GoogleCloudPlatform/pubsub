// Copyright 2017 Google Inc.
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

package com.google.pubsub.clients.tests;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Example main class to start up several ProducerThreads with a specified topic.
 * Number of threads will also be specified by a command-line argument.
 * arg[0] = topic, arg[1] = number of threads, arg[2] = number of messages to send.
 * Topic is required; if number of threads is not specified, 2 is the default.
 * If number of messages is not specified, 1 is the default.
 */
public class ProducerThreadPool {
  private static final Logger log = LoggerFactory.getLogger(ProducerThreadPool.class);

  public static void main(String[] args) {
    Preconditions.checkArgument(args.length > 0);
    ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder();
    threadFactoryBuilder.setNameFormat("pubsub-producer-thread");
    threadFactoryBuilder.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        log.error(t + " throws exception: " + e);
      }
    });

    ExecutorService executor = Executors.newCachedThreadPool(threadFactoryBuilder.build());

    Properties props = new Properties();
    props.putAll(new ImmutableMap.Builder<>()
        .put("project", "dataproc-kafka-test")
        .put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("acks", "all")
        .put("batch.size", "1000")
        .put("linger.ms", "1")
        .build()
    );

    int numThreads = 2;
    if (args.length > 1) {
      numThreads = Integer.parseInt(args[1]);
    }
    int numMessages = 1;
    if (args.length > 2) {
      numMessages = Integer.parseInt(args[2]);
    }

    for (int i = 0; i < numThreads; i++) {
      Runnable worker = new ProducerThread("" + i, props, args[0], numMessages);
      executor.execute(worker);
    }

    executor.shutdown();
    try {
      if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
        executor.shutdownNow();
        if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
          log.error("Executor did not terminate");
        }
      }
    } catch (InterruptedException ie) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
