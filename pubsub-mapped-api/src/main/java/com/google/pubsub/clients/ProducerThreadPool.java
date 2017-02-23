package com.google.pubsub.clients;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.pubsub.clients.producer.PubsubProducer;
import java.util.Properties;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ProducerThreadPool {
  private static final Logger log = LoggerFactory.getLogger(ProducerThreadPool.class);

  public static void main(String[] args) throws IOException {
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
        .put("batch.size", "1")
        .put("linger.ms", "1")
        .build()
    );

    for (int i = 0; i < 1; i++) {
      Runnable worker = new ProducerThread("" + i, props, args[0]);
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