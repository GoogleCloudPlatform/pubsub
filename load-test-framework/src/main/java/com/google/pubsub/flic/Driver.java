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
package com.google.pubsub.flic;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.monitoring.v3.Monitoring;
import com.google.api.services.monitoring.v3.model.ListTimeSeriesResponse;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import com.google.pubsub.flic.common.Utils;
import com.google.pubsub.flic.controllers.Client;
import com.google.pubsub.flic.controllers.Client.ClientType;
import com.google.pubsub.flic.controllers.ClientParams;
import com.google.pubsub.flic.controllers.GCEController;
import com.google.pubsub.flic.processing.Comparison;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.logging.LogManager;

/**
 * Drives the execution of the framework through command line arguments.
 */
public class Driver {
  private final static Logger log = LoggerFactory.getLogger(Driver.class);
  @Parameter(
      names = {"--help"},
      help = true
  )
  private boolean help = false;
  @Parameter(
      names = {"--dump_data", "-d"},
      description = "Whether to dump relevant message data (only when consuming messages)."
  )
  private boolean dumpData = false;
  @Parameter(
      names = {"--cps_publisher_count"},
      description = "Number of CPS publishers to start."
  )
  private int cpsPublisherCount = 1;
  @Parameter(
      names = {"--cps_subscriber_count"},
      description = "Number of CPS subscribers to start per ."
  )
  private int cpsSubscriberCount = 1;
  @Parameter(
      names = {"--kafka_publisher_count"},
      description = "Number of Kafka publishers to start."
  )
  private int kafkaPublisherCount = 0;
  @Parameter(
      names = {"--kafka_subscriber_count"},
      description = "Number of Kafka subscribers to start."
  )
  private int kafkaSubscriberCount = 0;
  @Parameter(
      names = {"--num_messages", "-n"},
      description = "Total number of messages to publish or consume.",
      validateWith = Utils.GreaterThanZeroValidator.class
  )
  private int numMessages = 10000;
  @Parameter(
      names = {"--message_size", "-m"},
      description = "Message size in bytes (only when publishing messages).",
      validateWith = Utils.GreaterThanZeroValidator.class
  )
  private int messageSize = 1000;
  @Parameter(
      names = {"--loadtest_seconds"},
      description = "Duration of the load test, in seconds.",
      validateWith = Utils.GreaterThanZeroValidator.class
  )
  private int loadtestLengthSeconds = 300;
  @Parameter(
      names = {"--project"},
      required = true,
      description = "Cloud Pub/Sub project name."
  )
  private String project = "";
  @Parameter(
      names = {"--batch_size", "-b"},
      description = "Number of messages to batch per publish request.",
      validateWith = Utils.GreaterThanZeroValidator.class
  )
  private int batchSize = 1000;
  @Parameter(
      names = {"--response_threads", "-r"},
      description = "Number of threads to use to handle response callbacks.",
      validateWith = Utils.GreaterThanZeroValidator.class
  )
  private int numResponseThreads = 1;

  @Parameter(
      names = {"--subscriber_fanout"},
      description = "Number of subscription ids to use for each topic. Must be at least 1.",
      validateWith = Utils.GreaterThanZeroValidator.class
  )
  private int subscriberFanout = 1;

  public static void main(String[] args) {
    // Turns off all java.util.logging.
    LogManager.getLogManager().reset();
    Driver driver = new Driver();
    JCommander jCommander = new JCommander(driver);
    Comparison comparison = new Comparison();
    jCommander.addCommand(Comparison.COMMAND, comparison);
    jCommander.parse(args);
    // Compare for correctness
    if (driver.help) {
      jCommander.usage();
      return;
    }
    if (jCommander.getParsedCommand() != null &&
        jCommander.getParsedCommand().equals(Comparison.COMMAND)) {
      try {
        comparison.compare();
      } catch (Exception e) {
        log.error("You must specify both --file1 and --file2 for" + Comparison.COMMAND);
        System.exit(1);
      }
      return;
    }
    driver.run();
  }

  private void run() {
    // for now we'll just use 1 region. But let's give a param for fanout. Then:
    // topic is just our topic which we need to create here. (well recreate, so no backlog)
    // subscription is a prefix based on fanout.
    try {
      Map<String, Map<ClientParams, Integer>> clientTypes = ImmutableMap.of(
          "us-central1-a", new HashMap<>());
      Preconditions.checkArgument(subscriberFanout > 0);
      Preconditions.checkArgument(
          cpsPublisherCount > 0 ||
              cpsSubscriberCount > 0 ||
              kafkaPublisherCount > 0 ||
              kafkaSubscriberCount > 0
      );
      for (int i = 0; i < subscriberFanout; ++i) {
        clientTypes.get("us-central1-a").put(new ClientParams(ClientType.CPS_GRPC_PUBLISHER, null),
            cpsPublisherCount / subscriberFanout);
        clientTypes.get("us-central1-a").put(new ClientParams(ClientType.CPS_GRPC_SUBSCRIBER, "grpc-subscription" + i),
            cpsSubscriberCount / subscriberFanout);
        clientTypes.get("us-central1-a").put(new ClientParams(ClientType.KAFKA_PUBLISHER, null),
            kafkaPublisherCount / subscriberFanout);
        clientTypes.get("us-central1-a").put(new ClientParams(ClientType.KAFKA_SUBSCRIBER, "kafka-subscription" + i),
            kafkaSubscriberCount / subscriberFanout);
      }
      Client.messageSize = messageSize;
      Client.requestRate = 1;
      Client.startTime = Timestamp.newBuilder().build();
      Client.loadtestLengthSeconds = loadtestLengthSeconds;
      Client.batchSize = batchSize;
      GCEController gceController =
          GCEController.newGCEController(project, clientTypes, Executors.newCachedThreadPool());
      gceController.initialize();
      gceController.startClients();
      SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
      dateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
      String startTime = dateFormatter.format(new Date());
      // Wait for the load test to finish.
      Thread.sleep(loadtestLengthSeconds * 1000);
      gceController.shutdown(new Exception("Loadtest completed."));

      // Print out latency measurements.
      HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
      JsonFactory jsonFactory = new JacksonFactory();
      GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
      if (credential.createScopedRequired()) {
        credential =
            credential.createScoped(
                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
      }
      Monitoring monitoring = new Monitoring.Builder(transport, jsonFactory, credential)
          .setApplicationName("Cloud Pub/Sub Loadtest Framework")
          .build();
      Monitoring.Projects.TimeSeries.List request = monitoring.projects().timeSeries().list("projects/" + project)
          .setFilter("metric.type = \"custom.googleapis.com/cloud-pubsub/loadclient/end_to_end_latency\"")
          .setIntervalStartTime(startTime)
          .setIntervalEndTime(dateFormatter.format(new Date()))
          .setAggregationAlignmentPeriod("60s")
          .setAggregationPerSeriesAligner("ALIGN_PERCENTILE_99");
      ListTimeSeriesResponse response;
      do {
        response = request.execute();
        response.getTimeSeries().forEach(timeSeries -> {
          log.info("Instance: " + timeSeries.getResource().getLabels().toString());
          timeSeries.getPoints().forEach(point -> {
            log.info(point.getInterval().getStartTime() + " to " + point.getInterval().getEndTime());
            log.info("Mean end to end latency: " + point.getValue().getDistributionValue().getMean());
          });
        });
      } while (response.getNextPageToken() != null);
    } catch (Throwable t) {
      log.error("An error occurred...", t);
      System.exit(1);
    }
  }
}
