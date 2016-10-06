package com.google.pubsub.clients.common;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Task implements Runnable {
  protected final MetricsHandler metricsHandler;
  protected AtomicInteger numberOfMessages = new AtomicInteger(0);

  protected Task(String project, String type, MetricsHandler.MetricName metricName) {
    this.metricsHandler = new MetricsHandler(project, type, metricName);
  }

  List<Long> getBucketValues() {
    return metricsHandler.getBucketValues();
  }

  int getNumberOfMessages() {
    return numberOfMessages.get();
  }
}
