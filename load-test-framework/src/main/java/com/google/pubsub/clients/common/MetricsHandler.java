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
package com.google.pubsub.clients.common;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.monitoring.v3.Monitoring;
import com.google.api.services.monitoring.v3.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.pubsub.flic.common.LoadtestProto;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.RequestAcceptEncoding;
import org.apache.http.client.protocol.ResponseContentEncoding;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A class that is used to record metrics related to the execution of the load tests, such metrics
 * are recorded using Google's Cloud Monitoring API.
 */
public class MetricsHandler {
  private static final Logger log = LoggerFactory.getLogger(MetricsHandler.class);
  private final String project;
  private final SimpleDateFormat dateFormatter;
  private final LatencyDistribution distribution;
  private final ScheduledExecutorService executor;
  private final String clientType;
  private final MonitoredResource monitoredResource;
  private final String startTime;
  private Monitoring monitoring;
  private MetricName metricName;

  public MetricsHandler(String project, String clientType, MetricName metricName) {
    this.project = project;
    this.clientType = clientType;
    this.metricName = metricName;
    dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    dateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    startTime = dateFormatter.format(new Date());
    distribution = new LatencyDistribution();
    monitoredResource = new MonitoredResource().setType("gce_instance");
    executor = Executors.newSingleThreadScheduledExecutor();
    executor.execute(this::initialize);
  }

  private void initialize() {
    synchronized (this) {
      try {
        HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
        if (credential.createScopedRequired()) {
          credential =
              credential.createScoped(
                  Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
        }
        monitoring = new Monitoring.Builder(transport, jsonFactory, credential)
            .setApplicationName("Cloud Pub/Sub Loadtest Framework")
            .build();
        String zoneId;
        String instanceId;
        try {
          DefaultHttpClient httpClient = new DefaultHttpClient();
          httpClient.addRequestInterceptor(new RequestAcceptEncoding());
          httpClient.addResponseInterceptor(new ResponseContentEncoding());

          HttpConnectionParams.setConnectionTimeout(httpClient.getParams(), 30000);
          HttpConnectionParams.setSoTimeout(httpClient.getParams(), 30000);
          HttpConnectionParams.setSoKeepalive(httpClient.getParams(), true);
          HttpConnectionParams.setStaleCheckingEnabled(httpClient.getParams(), false);
          HttpConnectionParams.setTcpNoDelay(httpClient.getParams(), true);

          SchemeRegistry schemeRegistry = httpClient.getConnectionManager().getSchemeRegistry();
          schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
          schemeRegistry.register(new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));
          httpClient.setKeepAliveStrategy((response, ctx) -> 30);
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

        monitoredResource.setLabels(ImmutableMap.of(
            "project_id", project,
            "instance_id", instanceId,
            "zone", zoneId
        ));
        createMetrics();
        executor.scheduleAtFixedRate(this::reportMetrics, 30, 30, TimeUnit.SECONDS);
      } catch (IOException e) {
        log.error("Unable to initialize MetricsHandler, trying again.", e);
        executor.execute(this::initialize);
      } catch (GeneralSecurityException e) {
        log.error("Unable to initialize MetricsHandler permanently, credentials error.", e);
      }
    }
  }

  private void createMetrics() {
    try {
      MetricDescriptor metricDescriptor = new MetricDescriptor()
          .setType("custom.googleapis.com/cloud-pubsub/loadclient/" + metricName);
      metricDescriptor.setDisplayName(metricName.toPrettyString())
          .setDescription(metricName.toPrettyString())
          .setName(metricDescriptor.getType())
          .setLabels(Collections.singletonList(new LabelDescriptor()
              .setKey("client_type")
              .setDescription("The type of client reporting latency.")
              .setValueType("STRING")))
          .setMetricKind("CUMULATIVE")
          .setValueType("DISTRIBUTION")
          .setUnit("ms");
      monitoring.projects().metricDescriptors().create("projects/" + project, metricDescriptor).execute();
    } catch (Exception e) {
      log.info("Metrics already exist.");
    }
  }

  public void recordLatency(long latencyMs) {
    synchronized (this) {
      distribution.recordLatency(latencyMs);
    }
  }

  private void reportMetrics() {
    CreateTimeSeriesRequest request;
    synchronized (this) {
      request = new CreateTimeSeriesRequest().setTimeSeries(Collections.singletonList(
          new TimeSeries()
              .setMetric(new Metric()
                  .setType("custom.googleapis.com/cloud-pubsub/loadclient/" + metricName)
                  .setLabels(ImmutableMap.of("client_type", clientType)))
              .setMetricKind("CUMULATIVE")
              .setValueType("DISTRIBUTION")
              .setPoints(Collections.singletonList(new Point()
                  .setValue(new TypedValue()
                      .setDistributionValue(new Distribution()
                          .setBucketCounts(distribution.getBucketValues())
                          .setCount(distribution.getCount())
                          .setMean(distribution.getMean())
                          .setSumOfSquaredDeviation(distribution.getSumOfSquareDeviations())
                          .setBucketOptions(new BucketOptions()
                              .setExplicitBuckets(new Explicit().setBounds(LatencyDistribution.LATENCY_BUCKETS)))))
                  .setInterval(new TimeInterval()
                      .setStartTime(startTime)
                      .setEndTime(dateFormatter.format(new Date())))))
              .setResource(monitoredResource)));
    }
    try {
      monitoring.projects().timeSeries().create("projects/" + project, request).execute();
    } catch (IOException e) {
      log.error("Error reporting latency.", e);
    }
  }

  public LoadtestProto.Distribution getDistribution() {
    return distribution.toDistribution();
  }

  public enum MetricName {
    END_TO_END_LATENCY,
    PUBLISH_ACK_LATENCY;

    @Override
    public String toString() {
      return name().toLowerCase();
    }

    public String toPrettyString() {
      return name().toLowerCase().replace('-', ' ');
    }
  }

  private static class LatencyDistribution {
    static final List<Double> LATENCY_BUCKETS =
        ImmutableList.of(
            0.0,
            1.0,
            5.0,
            10.0,
            20.0,
            40.0,
            60.0,
            80.0,
            100.0,
            150.0,
            200.0,
            500.0,
            1000.0,
            2000.0,
            3000.0,
            10000.0,
            20000.0,
            100000.0,
            400000.0,
            1000000.0,
            10000000.0,
            100000000.0,
            1000000000.0,
            (double) Integer.MAX_VALUE
        );
    private final List<Long> bucketValues = new ArrayList<>(LATENCY_BUCKETS.size());
    private long count = 0;
    private double mean = 0;
    private double sumOfSquaredDeviation = 0;

    LatencyDistribution() {
    }

    synchronized LoadtestProto.Distribution toDistribution() {
      return LoadtestProto.Distribution.newBuilder()
          .addAllBucketValues(bucketValues)
          .setCount(count)
          .setMean(mean)
          .setSumOfSquaredDeviation(sumOfSquaredDeviation)
          .build();
    }

    long getCount() {
      return count;
    }

    double getSumOfSquareDeviations() {
      return sumOfSquaredDeviation;
    }

    double getMean() {
      return mean;
    }

    List<Long> getBucketValues() {
      return bucketValues;
    }

    void recordLatency(long latencyMs) {
      synchronized (this) {
        count++;
        double dev = latencyMs - mean;
        mean += dev / count;
        sumOfSquaredDeviation += dev * (latencyMs - mean);
      }

      boolean bucketFound = false;
      for (int i = 0; i < LATENCY_BUCKETS.size(); i++) {
        double bucket = LATENCY_BUCKETS.get(i);
        if (latencyMs < bucket) {
          synchronized (this) {
            bucketValues.set(i, bucketValues.get(i) + 1);
          }
          bucketFound = true;
          break;
        }
      }
      if (!bucketFound) {
        synchronized (this) {
          int maxBucket = LATENCY_BUCKETS.size() - 1;
          bucketValues.set(maxBucket, bucketValues.get(maxBucket) + 1);
        }
      }
    }
  }

}
