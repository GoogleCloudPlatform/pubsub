package com.google.pubsub.clients.common;

import java.util.List;

public abstract class Task implements Runnable {
  protected final MetricsHandler metricsHandler;

  protected Task(String project, String type, MetricsHandler.MetricName metricName) {
    this.metricsHandler = new MetricsHandler(project, type, metricName);
  }

  List<Long> getBucketValues() {
    return metricsHandler.getBucketValues();
  }
}
