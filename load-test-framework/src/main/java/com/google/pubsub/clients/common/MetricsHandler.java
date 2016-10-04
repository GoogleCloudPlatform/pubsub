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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * A class that is used to record metrics related to the execution of the load tests, such metrics
 * are recorded using Google's Cloud Monitoring API.
 */
public class MetricsHandler {
  private static final Logger log = LoggerFactory.getLogger(MetricsHandler.class);
  private static final String END_TO_END_LATENCY_METRIC_NAME = "end_to_end_latency";
  private static final String PUBLISH_LATENCY_METRIC_NAME = "publish_latency";
  private Monitoring monitoring;
  private String project;
  private Executor executor;
  private SimpleDateFormat dateFormatter;
  private MonitoredResource monitoredResource;

  public MetricsHandler(String project) throws IOException, GeneralSecurityException {
    this.project = project;
    dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    dateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
    if (credential.createScopedRequired()) {
      credential =
          credential.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }
    this.executor = Executors.newScheduledThreadPool(5,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("load-thread").build());
    monitoring = new Monitoring.Builder(transport, jsonFactory, credential)
        .setApplicationName("Cloud Pub/Sub Loadtest Framework")
        .build();
    monitoredResource = new MonitoredResource();
    monitoredResource.setType("gce_instance");
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
  }

  private void createMetrics() {
    try {
      MetricDescriptor metricDescriptor = new MetricDescriptor();
      metricDescriptor.setType("custom.googleapis.com/cloud-pubsub/loadclient/" + END_TO_END_LATENCY_METRIC_NAME);
      metricDescriptor.setDisplayName("end to end latency");
      metricDescriptor.setDescription("End to end latency metric");
      metricDescriptor.setName(metricDescriptor.getType());
      metricDescriptor.setLabels(new ArrayList<>());
      metricDescriptor.setMetricKind("GAUGE");
      metricDescriptor.setValueType("INT64");
      metricDescriptor.setUnit("ms");
      monitoring.projects().metricDescriptors().create("projects/" + project, metricDescriptor).execute();
      metricDescriptor.setType("custom.googleapis.com/cloud-pubsub/loadclient/" + PUBLISH_LATENCY_METRIC_NAME);
      metricDescriptor.setDisplayName("publish latency");
      metricDescriptor.setDescription("Publish latency metric");
      metricDescriptor.setName(metricDescriptor.getType());
      monitoring.projects().metricDescriptors().create("projects/" + project, metricDescriptor).execute();
    } catch (Exception e) {
      log.info("Metrics already exist.");
    }
  }

  // We can only report one at a time, since any sent out of order will be rejected by Stackdriver.
  public void recordEndToEndLatency(long latencyMs) {
    synchronized (END_TO_END_LATENCY_METRIC_NAME) {
      recordLatency(latencyMs, END_TO_END_LATENCY_METRIC_NAME);
    }
  }

  // We can only report one at a time, since any sent out of order will be rejected by Stackdriver.
  public void recordPublishLatency(long latencyMs) {
    synchronized (PUBLISH_LATENCY_METRIC_NAME) {
      recordLatency(latencyMs, PUBLISH_LATENCY_METRIC_NAME);
    }
  }

  private void recordLatency(long latencyMs, String name) {
    log.debug("Adding a latency: " + latencyMs);
    String now = dateFormatter.format(new Date());
    try {
      monitoring.projects().timeSeries().create("projects/" + project,
          new CreateTimeSeriesRequest().setTimeSeries(Collections.singletonList(new TimeSeries()
              .setMetric(new Metric()
                  .setType("custom.googleapis.com/cloud-pubsub/loadclient/" + name)
                  .setLabels(new HashMap<>()))
              .setMetricKind("GAUGE")
              .setValueType("INT64")
              .setPoints(Collections.singletonList(
                  new Point()
                      .setValue(new TypedValue()
                          .setInt64Value(latencyMs))
                      .setInterval(new TimeInterval()
                          .setStartTime(now)
                          .setEndTime(now)))
              ).setResource(monitoredResource)))).execute();
    } catch (IOException e) {
      log.error("Error reporting latency.", e);
    }
  }
}
