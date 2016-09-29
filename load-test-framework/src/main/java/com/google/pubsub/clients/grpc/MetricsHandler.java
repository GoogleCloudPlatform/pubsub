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
package com.google.pubsub.clients.grpc;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A class that is used to record metrics related to the execution of the load tests, such metrics
 * are recorded using Google's Cloud Monitoring API.
 */
public class MetricsHandler {

  static final int[] LATENCY_BUCKETS =
      new int[]{
          0,
          1,
          5,
          10,
          20,
          40,
          60,
          80,
          100,
          150,
          200,
          500,
          1000,
          2000,
          3000,
          10000,
          20000,
          100000,
          400000,
          1000000,
          10000000,
          100000000,
          1000000000,
          Integer.MAX_VALUE
      };
  private static final Logger log = LoggerFactory.getLogger(MetricsHandler.class);
  private static final String REQUESTS_COUNT_METRIC_NAME = "request_count";
  private static final String END_TO_END_LATENCY_METRIC_NAME = "end_to_end_latency";
  private static final String PUBLISH_ACK_LATENCY_METRIC_NAME = "publish_ack_latency";
  private static final int MAX_HTTP_CONNECTIONS = 5;
  private final HttpClient httpClient;
  private final String requestCountTimeSeriesTemplate;
  private final String latencyTimeSeriesTemplate;
  private final String timeSeriesPath;
  private final ScheduledExecutorService executor;
  private final String startTime;
  private final Map<RequestCountKey, AtomicInteger> requestCount;
  private final LatencyDistribution endToEndLatencyDistribution;
  private final LatencyDistribution publishAckLatencyDistribution;
  private final SimpleDateFormat dateFormatter;
  private final String metricsDescriptorsPath;
  private final int metricsReportIntervalSecs;
  private final AccessTokenProvider accessTokenProvider;
  private AtomicBoolean countChanged;
  private String zoneId;
  private String instanceId;

  public MetricsHandler(
      String project, int metricsReportIntervalSecs, AccessTokenProvider accessTokenProvider) {
    this.accessTokenProvider = Preconditions.checkNotNull(accessTokenProvider);
    executor =
        Executors.newScheduledThreadPool(
            MAX_HTTP_CONNECTIONS,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("load-thread").build());
    httpClient = HttpClient.builder().maxConnections(MAX_HTTP_CONNECTIONS).build();

    metricsDescriptorsPath =
        "https://monitoring.googleapis.com/v3/projects/"
            + Preconditions.checkNotNull(project)
            + "/metricDescriptors";
    dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    dateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    startTime = dateFormatter.format(new Date());
    requestCount = new HashMap<>();
    countChanged = new AtomicBoolean(false);
    endToEndLatencyDistribution = new LatencyDistribution();
    publishAckLatencyDistribution = new LatencyDistribution();
    this.metricsReportIntervalSecs = metricsReportIntervalSecs;
    timeSeriesPath = "https://monitoring.googleapis.com/v3/projects/" + project + "/timeSeries";
    requestCountTimeSeriesTemplate =
        "{"
            + "\"metric\":{"
            + "\"type\":\"custom.googleapis.com/cloud-pubsub/loadclient/"
            + REQUESTS_COUNT_METRIC_NAME + "\","
            + "\"labels\": {"
            + "\"operation\": \"%s\","
            + "\"response\": \"%s\""
            + "}"
            + "},"
            + "\"resource\": {"
            + "\"type\": \"gce_instance\","
            + "\"labels\": {"
            + "\"project_id\": \"" + project + "\","
            + "\"instance_id\": \"%s\","
            + "\"zone\": \"%s\""
            + "}"
            + "},"
            + "\"points\": ["
            + "{"
            + "\"interval\":{"
            + "\"startTime\":\"" + startTime + "\","
            + "\"endTime\":\"%s\""
            + "},"
            + "\"value\": {"
            + "\"int64Value\":%d"
            + "}"
            + "}]"
            + "}";
    latencyTimeSeriesTemplate =
        "{"
            + "\"metric\":{"
            + "\"type\":\"custom.googleapis.com/cloud-pubsub/loadclient/%s\","
            + "},"
            + "\"resource\": {"
            + "\"type\": \"gce_instance\","
            + "\"labels\": {"
            + "\"project_id\": \"" + project + "\","
            + "\"instance_id\": \"%s\","
            + "\"zone\": \"%s\""
            + "}"
            + "},"
            + "\"points\": ["
            + "{"
            + "\"interval\":{"
            + "\"startTime\":\"" + startTime + "\","
            + "\"endTime\":\"%s\""
            + "},"
            + "\"value\": {"
            + "\"distributionValue\":{"
            + "\"count\": %d,"
            + "\"mean\": %f,"
            + "\"sumOfSquaredDeviation\": %f,"
            + "\"bucketOptions\": {"
            + "\"explicitBuckets\": {"
            + "\"bounds\":" + Arrays.toString(LATENCY_BUCKETS)
            + "}"
            + "},"
            + "\"bucketCounts\":%s"
            + "}"
            + "}"
            + "}]"
            + "}";

    try {
      HttpGet zoneIdRequest =
          new HttpGet("http://metadata.google.internal/computeMetadata/v1/instance/zone");
      zoneIdRequest.setHeader("Metadata-Flavor", "Google");
      HttpResponse zoneIdResponse = httpClient.execute(zoneIdRequest);
      String tempZoneId = EntityUtils.toString(zoneIdResponse.getEntity());
      if (tempZoneId.lastIndexOf("/") >= 0) {
        zoneId = tempZoneId.substring(tempZoneId.lastIndexOf("/") + 1);
      } else {
        zoneId = tempZoneId;
      }
      HttpGet instanceIdRequest =
          new HttpGet("http://metadata.google.internal/computeMetadata/v1/instance/id");
      instanceIdRequest.setHeader("Metadata-Flavor", "Google");
      HttpResponse instanceIdResponse = httpClient.execute(instanceIdRequest);
      instanceId = EntityUtils.toString(instanceIdResponse.getEntity());
    } catch (IOException e) {
      log.info(
          "Unable to connect to metadata server, assuming not on GCE, setting "
              + "defaults for instance and zone.");
      instanceId = "local";
      zoneId = "us-east1-b";  // Must use a valid cloud zone even if running local.
    }
  }

  private static boolean isSuccessStatus(int statusCode) {
    return statusCode >= 200 && statusCode < 300;
  }

  public void initialize() {
    createMetrics();
  }

  public void startReporting() {
    executor.scheduleAtFixedRate(
        new Runnable() {
          @Override
          public void run() {
            try {
              reportMetrics();
            } catch (Exception e) {
              log.warn("Unable to log metrics", e);
            }
          }
        },
        metricsReportIntervalSecs,
        metricsReportIntervalSecs,
        TimeUnit.SECONDS);
  }

  private void createMetric(final String metricName, StringEntity requestBody) {
    try {
      log.debug("Posting metric create to: %s", metricsDescriptorsPath);
      HttpPost postRequest = new HttpPost(metricsDescriptorsPath);
      postRequest.addHeader(
          "Authorization", "Bearer " + accessTokenProvider.getAccessToken().getTokenValue());
      postRequest.setEntity(requestBody);
      HttpResponse createResponse = httpClient.execute(postRequest);
      boolean success = isSuccessStatus(createResponse.getStatusLine().getStatusCode());
      if (!success) {
        try {
          log.warn(
              "Failed to create metric %s, reason: %s, response: %s",
              metricName,
              createResponse.getStatusLine(),
              EntityUtils.toString(createResponse.getEntity()));
        } catch (IOException e) {
          log.warn("Unable to log metric creation error", e);
        }
      } else {
        log.info("Created metric: %s", metricName);
      }
    } catch (Exception e) {
      log.warn("Failed to create metric %s", metricName, e);
    }
  }

  private ListenableFuture<Boolean> createMetrics() {
    final SettableFuture<Boolean> resultFuture = SettableFuture.create();
    try {
      StringEntity requestsCountMetricSpec =
          new StringEntity(
              "{"
                  + "\"name\":\""
                  + "custom.googleapis.com/cloud-pubsub/loadclient/"
                  + REQUESTS_COUNT_METRIC_NAME
                  + "\","
                  + "\"description\":\"Count of requests sent by the client\","
                  + "\"displayName\":\"request count\","
                  + "\"type\":\"custom.googleapis.com/cloud-pubsub/loadclient/"
                  + REQUESTS_COUNT_METRIC_NAME
                  + "\","
                  + "\"metricKind\":\"CUMULATIVE\","
                  + "\"valueType\":\"INT64\","
                  + "\"unit\":\"requests\","
                  + "\"labels\":[{"
                  + "  \"key\":\"response\","
                  + "  \"valueType\":\"STRING\","
                  + "  \"description\":\"Request response HTTP code\"},"
                  + " {\"key\":\"operation\","
                  + "  \"valueType\":\"STRING\","
                  + "  \"description\":\"Request operation (publish, pull)\"}],"
                  + "}");
      createMetric(REQUESTS_COUNT_METRIC_NAME, requestsCountMetricSpec);

      StringEntity endToEndLatencyMetricSpec =
          new StringEntity(
              "{"
                  + "\"name\":\""
                  + "custom.googleapis.com/cloud-pubsub/loadclient/"
                  + END_TO_END_LATENCY_METRIC_NAME
                  + "\","
                  + "\"description\":\"End to end latency metric\","
                  + "\"displayName\":\"end to end latency\","
                  + "\"type\":\"custom.googleapis.com/cloud-pubsub/loadclient/"
                  + END_TO_END_LATENCY_METRIC_NAME
                  + "\","
                  + "\"metricKind\":\"CUMULATIVE\","
                  + "\"valueType\":\"DISTRIBUTION\","
                  + "\"unit\":\"ms\","
                  + "\"labels\":[],"
                  + "}");
      createMetric(END_TO_END_LATENCY_METRIC_NAME, endToEndLatencyMetricSpec);

      StringEntity publishAckLatencyMetricSpec =
          new StringEntity(
              "{"
                  + "\"name\":\""
                  + "custom.googleapis.com/cloud-pubsub/loadclient/"
                  + PUBLISH_ACK_LATENCY_METRIC_NAME
                  + "\","
                  + "\"description\":\"End to end latency metric\","
                  + "\"displayName\":\"end to end latency\","
                  + "\"type\":\"custom.googleapis.com/cloud-pubsub/loadclient/"
                  + PUBLISH_ACK_LATENCY_METRIC_NAME
                  + "\","
                  + "\"metricKind\":\"CUMULATIVE\","
                  + "\"valueType\":\"DISTRIBUTION\","
                  + "\"unit\":\"ms\","
                  + "\"labels\":[],"
                  + "}");
      createMetric(PUBLISH_ACK_LATENCY_METRIC_NAME, publishAckLatencyMetricSpec);
    } catch (Exception e) {
      resultFuture.set(false);
      log.warn("Failed to pull messages", e);
    }
    return resultFuture;
  }

  public void recordRequestCount(String resource, String operation, int responseCode, int count) {
    RequestCountKey key = RequestCountKey.of(resource, operation, responseCode);
    synchronized (requestCount) {
      if (!requestCount.containsKey(key)) {
        requestCount.put(key, new AtomicInteger());
      }
    }
    requestCount.get(key).addAndGet(count);
    countChanged.set(true);
  }

  public void recordEndToEndLatency(long latencyMs) {
    log.debug("Adding a end to end latency: %s", latencyMs);
    endToEndLatencyDistribution.recordLatency(latencyMs);
  }

  public void recordPublishAckLatency(long latencyMs) {
    log.debug("Adding a publish ack latency: %s", latencyMs);
    publishAckLatencyDistribution.recordLatency(latencyMs);
  }

  private String buildRequestCountTimeSeries(RequestCountKey key, int count) {
    String endTime = dateFormatter.format(new Date());
    return String.format(
        requestCountTimeSeriesTemplate,
        key.operation,
        key.responseCode,
        instanceId,
        zoneId,
        endTime,
        count);
  }

  private String buildEndToEndLatencyTimeSeries() {
    String endTime = dateFormatter.format(new Date());
    return String.format(
        latencyTimeSeriesTemplate,
        END_TO_END_LATENCY_METRIC_NAME,
        instanceId,
        zoneId,
        endTime,
        endToEndLatencyDistribution.getCount(),
        endToEndLatencyDistribution.getMean(),
        endToEndLatencyDistribution.getSumOfSquareDeviations(),
        Arrays.toString(endToEndLatencyDistribution.getBucketValues()));
  }

  private String buildPublishAckLatencyTimeSeries() {
    String endTime = dateFormatter.format(new Date());
    return String.format(
        latencyTimeSeriesTemplate,
        PUBLISH_ACK_LATENCY_METRIC_NAME,
        instanceId,
        zoneId,
        endTime,
        publishAckLatencyDistribution.getCount(),
        publishAckLatencyDistribution.getMean(),
        publishAckLatencyDistribution.getSumOfSquareDeviations(),
        Arrays.toString(publishAckLatencyDistribution.getBucketValues()));
  }

  private void reportMetrics() {
    boolean localCountChanged = countChanged.compareAndSet(true, false);

    if (localCountChanged) {
      try {
        StringBuilder reportMetricsRequest = new StringBuilder("{\"timeSeries\": [");
        boolean first = true;
        for (Map.Entry<RequestCountKey, AtomicInteger> countEntry : requestCount.entrySet()) {
          if (!first) {
            reportMetricsRequest.append(",");
          } else {
            first = false;
          }
          reportMetricsRequest.append(
              buildRequestCountTimeSeries(countEntry.getKey(), countEntry.getValue().get()));
        }

        if (endToEndLatencyDistribution.getCount() > 0) {
          reportMetricsRequest.append(",");
          reportMetricsRequest.append(buildEndToEndLatencyTimeSeries());
        }

        if (publishAckLatencyDistribution.getCount() > 0) {
          reportMetricsRequest.append(",");
          reportMetricsRequest.append(buildPublishAckLatencyTimeSeries());
        }

        reportMetricsRequest.append("]}");

        log.debug("Reporting: %s", reportMetricsRequest.toString());
        StringEntity requestBody = new StringEntity(reportMetricsRequest.toString());
        HttpPost postRequest = new HttpPost(timeSeriesPath);
        postRequest.addHeader(
            "Authorization", "Bearer " + accessTokenProvider.getAccessToken().getTokenValue());
        postRequest.setEntity(requestBody);

        httpClient.executeFuture(
            postRequest,
            new ResponseHandler<Boolean>() {
              @Override
              public Boolean handleResponse(HttpResponse response) {
                boolean success = isSuccessStatus(response.getStatusLine().getStatusCode());
                if (!success) {
                  try {
                    log.warn(
                        "Failed to report request count metric, reason: %s. Response: %s",
                        response.getStatusLine(), EntityUtils.toString(response.getEntity()));
                  } catch (IOException e) {
                    log.warn(
                        "Failed to log invalid metric report request, reason: %s",
                        response.getStatusLine(), e);
                  }
                } else {
                  log.debug("Metrics report OK");
                }
                return success;
              }
            });
      } catch (Exception e) {
        log.warn("Unable to report metric values", e);
      }
    }
  }

  private static final class RequestCountKey {
    private final String resource;
    private final String operation;
    private final int responseCode;

    private RequestCountKey(String resource, String operation, int responseCode) {
      this.operation = Preconditions.checkNotNull(operation);
      this.resource = Preconditions.checkNotNull(resource);
      this.responseCode = responseCode;
    }

    public static RequestCountKey of(String resource, String operation, int responseCode) {
      return new RequestCountKey(resource, operation, responseCode);
    }

    @Override
    public int hashCode() {
      return operation.hashCode() + resource.hashCode() + responseCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof RequestCountKey)) {
        return false;
      }
      RequestCountKey other = (RequestCountKey) obj;
      return other.operation.equals(operation)
          && other.resource.equals(resource)
          && other.responseCode == responseCode;
    }
  }

  private static class LatencyDistribution {
    private long count = 0;
    private double mean = 0;
    private double sumOfSquaredDeviation = 0;
    private int[] bucketValues = new int[LATENCY_BUCKETS.length];

    public LatencyDistribution() {
    }

    public long getCount() {
      return count;
    }

    public double getSumOfSquareDeviations() {
      return sumOfSquaredDeviation;
    }

    public double getMean() {
      return mean;
    }

    public int[] getBucketValues() {
      return bucketValues;
    }

    public void recordLatency(long latencyMs) {
      synchronized (this) {
        count++;
        double dev = latencyMs - mean;
        mean += dev / count;
        sumOfSquaredDeviation += dev * (latencyMs - mean);
      }

      boolean bucketFound = false;
      for (int i = 0; i < LATENCY_BUCKETS.length; i++) {
        int bucket = LATENCY_BUCKETS[i];
        if (latencyMs < bucket) {
          synchronized (this) {
            bucketValues[i]++;
          }
          bucketFound = true;
          break;
        }
      }
      if (!bucketFound) {
        synchronized (this) {
          bucketValues[LATENCY_BUCKETS.length - 1]++;
        }
      }
    }
  }
}
