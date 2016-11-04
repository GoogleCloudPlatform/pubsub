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

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.monitoring.v3.Monitoring;
import com.google.api.services.monitoring.v3.model.ListTimeSeriesResponse;
import com.google.api.services.monitoring.v3.model.Point;
import com.google.api.services.monitoring.v3.model.TimeSeries;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.protobuf.Timestamp;
import com.google.pubsub.flic.common.LatencyDistribution;
import com.google.pubsub.flic.controllers.Client;
import com.google.pubsub.flic.controllers.Client.ClientType;
import com.google.pubsub.flic.controllers.ClientParams;
import com.google.pubsub.flic.controllers.Controller;
import com.google.pubsub.flic.controllers.GCEController;
import com.google.pubsub.flic.output.SheetsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

/**
 * Drives the execution of the framework through command line arguments.
 */
class Driver {
  private static final Logger log = LoggerFactory.getLogger(Driver.class);
  @Parameter(
      names = {"--help"},
      help = true
  )
  private boolean help = false;
  @Parameter(
      names = {"--cps_publisher_count"},
      description = "Number of CPS publishers to start."
  )
  private int cpsPublisherCount = 1;
  @Parameter(
      names = {"--cps_subscriber_count"},
      description =
          "Number of CPS subscribers to start. If this is not divisible by cps_subscription_fanout"
              + ", we will round down to the closest multiple of cps_subscription_fanout."
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
      names = {"--message_size", "-m"},
      description = "Message size in bytes (only when publishing messages).",
      validateWith = GreaterThanZeroValidator.class
  )
  private int messageSize = 100000;
  @Parameter(
      names = {"--loadtest_seconds"},
      description = "Duration of the load test, in seconds.",
      validateWith = GreaterThanZeroValidator.class
  )
  private int loadtestLengthSeconds = 180;
  @Parameter(
      names = {"--project"},
      required = true,
      description = "Google Cloud Platform project name."
  )
  private String project = "";
  @Parameter(
      names = {"--publish_batch_size"},
      description = "Number of messages to batch per publish request.",
      validateWith = GreaterThanZeroValidator.class
  )
  private int publishBatchSize = 10;
  @Parameter(
      names = {"--cps_max_messages_per_pull"},
      description = "Number of messages to return in each pull request.",
      validateWith = GreaterThanZeroValidator.class
  )
  private int cpsMaxMessagesPerPull = 10;
  @Parameter(
      names = {"--kafka_poll_length"},
      description = "Length of time, in milliseconds, to poll when subscribing with Kafka.",
      validateWith = GreaterThanZeroValidator.class
  )
  private int kafkaPollLength = 100;
  @Parameter(
      names = {"--cps_subscription_fanout"},
      description = "Number of subscriptions to create for each topic. Must be at least 1.",
      validateWith = GreaterThanZeroValidator.class
  )
  private int cpsSubscriptionFanout = 1;
  @Parameter(
      names = {"--broker"},
      description = "The network address of the Kafka broker."
  )
  private String broker;
  @Parameter(
      names = {"--request_rate"},
      description = "The rate at which each client will make requests (batches per second)."
  )
  private int requestRate = 10;
  @Parameter(
      names = {"--max_outstanding_requests"},
      description = "The maximum number of outstanding requests each client will allow."
  )
  private int maxOutstandingRequests = 20;
  @Parameter(
      names = {"--burn_in_duration_seconds"},
      description = "The duration, in seconds, to run without recording statistics to allow tuning."
  )
  private int burnInDurationSeconds = 120;
  @Parameter(
      names = {"--number_of_messages"},
      description = "The total number of messages to publish in the test. Enabling this will "
          + "override --loadtest_length_seconds. Enabling this flag will also enable the check for "
          + "message loss. If set less than 1, this flag is ignored."
  )
  private int numberOfMessages = 0;
  @Parameter(
      names = {"--max_publish_latency_test"},
      description = "In this test we will continuously run load tests with increasing request " +
          "rates until we hit max_publish_latency_millis. You must only provide a single type of " +
          "publisher to use this test. This uses the latency specified by " +
          "max_publish_latency_percentile as the bound to check."
  )
  private boolean maxPublishLatencyTest = false;
  @Parameter(
      names = {"--max_publish_latency_millis"},
      description = "This is the maximum latency in milliseconds allowed before terminating the " +
          "max_publish_latency_test."
  )
  private int maxPublishLatencyMillis = 0;
  @Parameter(
      names = {"--max_publish_latency_percentile"},
      description = "This sets the percentile to use when determining the latency for the " +
          "max_publish_latency_test. Defaults to 99."
  )
  private int maxPublishLatencyPercentile = 99;
  @Parameter(
      names = {"--max_subscriber_throughput_test"},
      description = "This test will continuously run load tests with greater publish request " +
          "rate until the subscriber can no longer keep up, and will let you know the maximum " +
          "throughput per subscribing client."
  )
  private boolean maxSubscriberThroughputTest = false;
  @Parameter(
      names = {"--max_subscriber_throughput_test_backlog"},
      description =
          "This is the size of the backlog, in messages, to allow during the " +
              "max_subscriber_throughput_test. "
  )
  private int maxSubscriberThroughputTestBacklog = 100;
  @Parameter(
      names = {"--spreadsheet_id"},
      description = "The id of the spreadsheet to which results are output."
  )
  private String spreadsheetId = "";
  @Parameter(
      names = {"--data_store_dir"},
      description = "The directory to store credentials for sheets output verification. Note: "
          + "sheets output is only turned on when spreadsheet_id is set to a non-empty value, so "
          + "data will only be stored to this directory if the spreadsheet_id flag is activated."
  )
  private String dataStoreDirectory =
      System.getProperty("user.home") + "/.credentials/sheets.googleapis.com-loadtest-framework";
  @Parameter(
      names = {"--resource_dir"},
      description = "The directory to look for resources to upload, if different than the default."
  )
  private String resourceDirectory = "src/main/resources/gce";
  @Parameter(
      names = {"--zone"},
      description = "The GCE zone in which to create client instances."
  )
  private String zone = "us-central1-a";

  public static void main(String[] args) {
    Driver driver = new Driver();
    JCommander jCommander = new JCommander(driver, args);
    if (driver.help) {
      jCommander.usage();
      return;
    }
    driver.run();
  }

  private void run() {
    try {
      Preconditions.checkArgument(
          cpsPublisherCount > 0
              || cpsSubscriberCount > 0
              || kafkaPublisherCount > 0
              || kafkaSubscriberCount > 0
          , "You must set at least one type of client greater than 0.");
      Preconditions.checkArgument(
          broker != null || (kafkaPublisherCount == 0 && kafkaSubscriberCount == 0),
          "If using Kafka you must provide the network address of your broker using the"
              + "--broker flag.");

      if (maxPublishLatencyTest) {
        Preconditions.checkArgument(kafkaPublisherCount > 0 ^ cpsPublisherCount > 0,
            "If max_publish_latency is specified, there can only be one type of publisher.");
      }
      GCEController.resourceDirectory = resourceDirectory;
      Map<ClientParams, Integer> clientParamsMap = new HashMap<>();
      clientParamsMap.putAll(ImmutableMap.of(
          new ClientParams(ClientType.CPS_GCLOUD_JAVA_PUBLISHER, null), cpsPublisherCount,
          new ClientParams(ClientType.KAFKA_PUBLISHER, null), kafkaPublisherCount,
          new ClientParams(ClientType.KAFKA_SUBSCRIBER, null), kafkaSubscriberCount
      ));
      // Each type of client will have its own topic, so each topic will get
      // cpsSubscriberCount subscribers cumulatively among each of the subscriptions.
      for (int i = 0; i < cpsSubscriptionFanout; ++i) {
        clientParamsMap.put(new ClientParams(ClientType.CPS_GCLOUD_JAVA_SUBSCRIBER,
            "gcloud-subscription" + i), cpsSubscriberCount / cpsSubscriptionFanout);
      }
      Client.messageSize = messageSize;
      Client.requestRate = 1;
      Client.loadtestLengthSeconds = loadtestLengthSeconds;
      Client.publishBatchSize = publishBatchSize;
      Client.maxMessagesPerPull = cpsMaxMessagesPerPull;
      Client.pollLength = kafkaPollLength;
      Client.broker = broker;
      Client.requestRate = requestRate;
      Client.maxOutstandingRequests = maxOutstandingRequests;
      Client.burnInTimeMillis = (Client.startTime.getSeconds() + burnInDurationSeconds) * 1000;
      Client.numberOfMessages = numberOfMessages;
      GCEController gceController = GCEController.newGCEController(
          project, ImmutableMap.of(zone, clientParamsMap),
          Executors.newScheduledThreadPool(500));

      // Start a thread to poll and output results.
      ScheduledExecutorService pollingExecutor = Executors.newSingleThreadScheduledExecutor();
      pollingExecutor.scheduleWithFixedDelay(() -> {
        synchronized (pollingExecutor) {
          log.info("===============================================");
          printStats(gceController.getStatsForAllClientTypes());
          log.info("===============================================");
        }
      }, 5, 5, TimeUnit.SECONDS);
      Map<ClientType, Controller.LoadtestStats> statsMap;
      AtomicDouble publishLatency = new AtomicDouble(0);

      final HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
      final JsonFactory jsonFactory = new JacksonFactory();
      final GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
      final Monitoring monitoring = new Monitoring.Builder(transport, jsonFactory, credential)
          .setApplicationName("Cloud Pub/Sub Loadtest Framework")
          .build();
      final SimpleDateFormat dateFormatter;
      dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
      dateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
      int highestRequestRate = 0;
      long backlogSize = 0;
      do {
        Client.startTime = Timestamp.newBuilder()
            .setSeconds(System.currentTimeMillis() / 1000 + 90).build();
        Date startDate = new Date();
        startDate.setTime(Client.startTime.getSeconds() * 1000);
        gceController.startClients();
        if (maxSubscriberThroughputTest) {
          gceController.waitForPublisherClients();
          ListTimeSeriesResponse response =
              monitoring.projects().timeSeries().list("projects/" + project)
                  .setFilter(
                      "metric.type = \"pubsub.googleapis.com/subscription/num_undelivered_messages\"")
                  .setIntervalStartTime(dateFormatter.format(startDate))
                  .setIntervalEndTime(dateFormatter.format(new Date()))
                  .execute();
          // Get most recent point.
          Point latestBacklogSize = null;
          for (TimeSeries timeSeries : response.getTimeSeries()) {
            for (Point point : timeSeries.getPoints()) {
              if (latestBacklogSize == null ||
                  dateFormatter.parse(point.getInterval().getStartTime()).after(
                      dateFormatter.parse(latestBacklogSize.getInterval().getStartTime()))) {
                latestBacklogSize = point;
              }
            }
          }
          if (latestBacklogSize != null) {
            backlogSize = latestBacklogSize.getValue().getInt64Value();
          }
          if (backlogSize > maxSubscriberThroughputTestBacklog) {
            log.info("We accumulated a backlog during this test, refer to the last run " +
                "for the maximum throughput attained before accumulating backlog." );
          }
        }
        // Wait for the load test to finish.
        gceController.waitForClients();
        statsMap = gceController.getStatsForAllClientTypes();
        if (maxPublishLatencyTest) {
          statsMap.forEach((type, stats) -> {
            if (type.isPublisher()) {
              publishLatency.set(LatencyDistribution
                  .getNthPercentileUpperBound(stats.bucketValues, maxPublishLatencyPercentile));
            }
          });
        }
        if (publishLatency.get() < maxPublishLatencyMillis) {
          highestRequestRate = Client.requestRate;
        }
        Client.requestRate *= 1.1;
        printStats(statsMap);
        if (spreadsheetId.length() > 0) {
          // Output results to common Google sheet
          SheetsService service = new SheetsService(dataStoreDirectory, gceController.getTypes());
          service.sendToSheets(spreadsheetId, statsMap);
        }
      } while ((maxPublishLatencyTest && publishLatency.get() < maxPublishLatencyMillis)
          || (maxSubscriberThroughputTest && backlogSize < maxSubscriberThroughputTestBacklog));
      synchronized (pollingExecutor) {
        pollingExecutor.shutdownNow();
      }
      if (maxPublishLatencyTest) {
        // This calculates the request rate of the last successful run.
        log.info("Maximum Request Rate: " + highestRequestRate);
      }
      gceController.shutdown(null);
      System.exit(0);
    } catch (Throwable t) {
      log.error("An error occurred...", t);
      System.exit(1);
    }
  }

  private void printStats(Map<ClientType, Controller.LoadtestStats> results) {
    results.forEach((type, stats) -> {
      log.info("Results for " + type + ":");
      log.info("50%: " + LatencyDistribution.getNthPercentile(stats.bucketValues, 50.0));
      log.info("99%: " + LatencyDistribution.getNthPercentile(stats.bucketValues, 99.0));
      log.info("99.9%: " + LatencyDistribution.getNthPercentile(stats.bucketValues, 99.9));
      // CPS Publishers report latency per batch message.
      log.info("Average throughput: "
          + new DecimalFormat("#.##").format(
              (double) LongStream.of(
                  stats.bucketValues).sum() / stats.runningSeconds * messageSize / 1000000.0)
          + " MB/s");
    });
  }

  /**
   * A validator that makes sure the parameter is an integer that is greater than 0.
   */
  public static class GreaterThanZeroValidator implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      try {
        int n = Integer.parseInt(value);
        if (n > 0) {
          return;
        }
        throw new NumberFormatException();
      } catch (NumberFormatException e) {
        throw new ParameterException(
            "Parameter " + name + " should be an int greater than 0 (found " + value + ")");
      }
    }
  }
}
