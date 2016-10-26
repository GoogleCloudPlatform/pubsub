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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import com.google.pubsub.flic.common.LatencyDistribution;
import com.google.pubsub.flic.controllers.Client;
import com.google.pubsub.flic.controllers.Client.ClientType;
import com.google.pubsub.flic.controllers.ClientParams;
import com.google.pubsub.flic.controllers.Controller;
import com.google.pubsub.flic.controllers.GCEController;
import com.google.pubsub.flic.output.SheetsService;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private int loadtestLengthSeconds = 60;
  @Parameter(
      names = {"--project"},
      required = true,
      description = "Google Cloud Platform project name."
  )
  private String project = "";
  @Parameter(
      names = {"--publish_batch_size"},
      description = "Number of messages to batch per Cloud Pub/Sub publish request.",
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
      description = "The rate at which each client will make requests."
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
  private int burnInDurationSeconds = 20;
  @Parameter(
      names = {"--number_of_messages"},
      description = "The total number of messages to publish in the test. Enabling this will "
          + "override --loadtest_length_seconds. Enabling this flag will also enable the check for "
          + "message loss. If set less than 1, this flag is ignored."
  )
  private int numberOfMessages = 0;
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
      );
      Preconditions.checkArgument(
          broker != null || (kafkaPublisherCount == 0 && kafkaSubscriberCount == 0));
      GCEController.resourceDirectory = resourceDirectory;
      Map<ClientParams, Integer> clientParamsMap = new HashMap<>();
      clientParamsMap.putAll(ImmutableMap.of(
          new ClientParams(ClientType.CPS_GCLOUD_PUBLISHER, null), cpsPublisherCount,
          new ClientParams(ClientType.KAFKA_PUBLISHER, null), kafkaPublisherCount,
          new ClientParams(ClientType.KAFKA_SUBSCRIBER, null), kafkaSubscriberCount
      ));
      // Each type of client will have its own topic, so each topic will get
      // cpsSubscriberCount subscribers cumulatively among each of the subscriptions.
      for (int i = 0; i < cpsSubscriptionFanout; ++i) {
        clientParamsMap.put(new ClientParams(ClientType.CPS_GCLOUD_SUBSCRIBER,
            "gcloud-subscription" + i), cpsSubscriberCount / cpsSubscriptionFanout);
      }
      Client.messageSize = messageSize;
      Client.requestRate = 1;
      Client.startTime = Timestamp.newBuilder()
          .setSeconds(System.currentTimeMillis() / 1000 + 90).build();
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
          project, ImmutableMap.of("us-central1-a", clientParamsMap),
          Executors.newScheduledThreadPool(500));
      gceController.startClients();

      // Start a thread to poll and output results.
      ScheduledExecutorService pollingExecutor = Executors.newSingleThreadScheduledExecutor();
      pollingExecutor.scheduleWithFixedDelay(() -> {
        synchronized (pollingExecutor) {
          log.info("===============================================");
          printStats(gceController.getStatsForAllClientTypes());
          log.info("===============================================");
        }
      }, 5, 5, TimeUnit.SECONDS);
      // Wait for the load test to finish.
      gceController.waitForClients();
      synchronized (pollingExecutor) {
        pollingExecutor.shutdownNow();
      }
      Map<ClientType, Controller.LoadtestStats> results = gceController.getStatsForAllClientTypes();
      printStats(results);
      
      if (spreadsheetId.length() > 0) {
        // Output results to common Google sheet
        SheetsService service = new SheetsService(dataStoreDirectory, gceController.getTypes());
        service.sendToSheets(spreadsheetId, results);
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
                  stats.bucketValues).sum() / stats.runningSeconds * messageSize / 1000000.0
                  * publishBatchSize) + " MB/s");
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
