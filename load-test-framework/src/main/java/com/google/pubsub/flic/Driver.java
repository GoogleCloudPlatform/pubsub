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
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.BaseConverter;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.monitoring.v3.Monitoring;
import com.google.api.services.monitoring.v3.model.ListTimeSeriesResponse;
import com.google.api.services.monitoring.v3.model.Point;
import com.google.api.services.monitoring.v3.model.TimeSeries;
import com.google.common.base.Ascii;
import com.google.common.base.CharMatcher;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.flic.common.LatencyDistribution;
import com.google.pubsub.flic.common.LoadtestProto.MessageIdentifier;
import com.google.pubsub.flic.controllers.Client;
import com.google.pubsub.flic.controllers.Client.ClientType;
import com.google.pubsub.flic.controllers.ClientParams;
import com.google.pubsub.flic.controllers.Controller;
import com.google.pubsub.flic.controllers.GCEController;
import com.google.pubsub.flic.controllers.MessageTracker;
import com.google.pubsub.flic.output.CsvOutput;
import com.google.pubsub.flic.output.GnuPlot;
import com.google.pubsub.flic.output.SheetsService;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.joda.time.DateTimeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Drives the execution of the framework through command line arguments. */
@Parameters(separators = "=")
public class Driver {
  private static final Logger log = LoggerFactory.getLogger(Driver.class);
  private static final int VERBOSE_STATS_POLL_SECONDS = 10;

  @Parameter(
    names = {"--help"},
    help = true
  )
  public boolean help = false;

  @Parameter(
    names = {"--cps_gcloud_java_publisher_count"},
    description = "Number of CPS publishers of this type to start."
  )
  private int cpsGcloudJavaPublisherCount = 0;

  @Parameter(
    names = {"--cps_gcloud_java_subscriber_count"},
    description = "Number of CPS subscribers of this type to start."
  )
  private int cpsGcloudJavaSubscriberCount = 0;

  @Parameter(
    names = {"--cps_experimental_java_publisher_count"},
    description = "Number of CPS publishers of this type to start."
  )
  private int cpsExperimentalJavaPublisherCount = 0;

  @Parameter(
    names = {"--cps_experimental_java_subscriber_count"},
    description = "Number of CPS subscribers of this type to start."
  )
  private int cpsExperimentalJavaSubscriberCount = 0;

  @Parameter(
    names = {"--cps_vtk_java_publisher_count"},
    description = "Number of CPS publishers of this type to start."
  )
  private int cpsVtkJavaPublisherCount = 0;

  @Parameter(
    names = {"--cps_gcloud_python_publisher_count"},
    description = "Number of CPS publishers of this type to start."
  )
  private int cpsGcloudPythonPublisherCount = 0;

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
    names = {"--loadtest_duration"},
    description = "Duration of the load test.",
    converter = DurationConverter.class
  )
  private Duration loadtestDuration = Durations.fromMillis(3 * 60 * 1000); // 3 min.

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
      names = {"--publish_batch_duration"},
      description = "The maximum duration to wait for more messages to batch together.",
      converter = DurationConverter.class
  )
  private Duration publishBatchDuration = Durations.fromMillis(0);

  @Parameter(
    names = {"--cps_max_messages_per_pull"},
    description = "Number of messages to return in each pull request.",
    validateWith = GreaterThanZeroValidator.class
  )
  private int cpsMaxMessagesPerPull = 10;

  @Parameter(
    names = {"--kafka_poll_length"},
    description = "Length of time to poll when subscribing with Kafka.",
    converter = DurationConverter.class
  )
  private Duration kafkaPollDuration = Durations.fromMillis(100);

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
    names = {"--burn_in_duration"},
    description = "The duration to run without recording statistics to allow tuning.",
    converter = DurationConverter.class
  )
  private Duration burnInDuration = Durations.fromMillis(2 * 60 * 1000); // 2 min.

  @Parameter(
    names = {"--number_of_messages"},
    description =
        "The total number of messages to publish in the test. Enabling this will "
            + "override --loadtest_length_seconds. Enabling this flag will also enable the check "
            + "for message loss. If set less than 1, this flag is ignored."
  )
  private int numberOfMessages = 0;

  @Parameter(
    names = {"--max_publish_latency_test"},
    description =
        "In this test we will continuously run load tests with increasing request "
            + "rates until we hit max_publish_latency_millis. You must only provide a single type "
            + "of publisher to use this test. This uses the latency specified by "
            + "max_publish_latency_percentile as the bound to check."
  )
  private boolean maxPublishLatencyTest = false;

  @Parameter(
    names = {"--max_publish_latency_millis"},
    description =
        "This is the maximum latency in milliseconds allowed before terminating the "
            + "max_publish_latency_test."
  )
  private int maxPublishLatencyMillis = 0;

  @Parameter(
    names = {"--max_publish_latency_percentile"},
    description =
        "This sets the percentile to use when determining the latency for the "
            + "max_publish_latency_test. Defaults to 99."
  )
  private int maxPublishLatencyPercentile = 99;

  @Parameter(
    names = {"--max_subscriber_throughput_test"},
    description =
        "This test will continuously run load tests with greater publish request "
            + "rate until the subscriber can no longer keep up, and will let you know the maximum "
            + "throughput per subscribing client."
  )
  private boolean maxSubscriberThroughputTest = false;

  @Parameter(
    names = {"--max_subscriber_throughput_test_backlog"},
    description =
        "This is the size of the backlog, in messages, to allow during the "
            + "max_subscriber_throughput_test. "
  )
  private int maxSubscriberThroughputTestBacklog = 100;

  @Parameter(
    names = {"--num_cores_test"},
    description =
        "This is a test where we will progressively run a max throughput test on GCE instances "
            + "with more cores, from 1 up to 16."
  )
  private boolean numCoresTest = false;

  @Parameter(
    names = {"--spreadsheet_id"},
    description = "The id of the spreadsheet to which results are output."
  )
  private String spreadsheetId = "";

  @Parameter(
    names = {"--data_store_dir"},
    description =
        "The directory to store credentials for sheets output verification. Note: "
            + "sheets output is only turned on when spreadsheet_id is set to a non-empty value, so "
            + "data will only be stored to this directory if the spreadsheet_id flag is activated."
  )
  private String dataStoreDirectory =
      System.getProperty("user.home") + "/.credentials/sheets.googleapis.com-loadtest-framework";

  @Parameter(
    names = {"--resource_dir"},
    description = "The directory to look for resources to upload, if different than the default."
  )
  private String resourceDirectory = "target/classes/gce";

  @Parameter(
    names = {"--zone"},
    description = "The GCE zone in which to create client instances."
  )
  private String zone = "us-central1-a";

  @Parameter(
    names = {"--cores"},
    description = "The number of cores to use on each GCE instance. Valid values are 1,2,4,8,16.",
    validateWith = CoresValidator.class
  )
  private int cores = 4;

  @Parameter(
      names = {"--verbose"},
      description = "When enabled, current statistics will be printed every 10 seconds."
  )
  private boolean verbose = false;

  private Controller controller;

  public static void main(String[] args) {
    Driver driver = new Driver();
    JCommander jCommander = new JCommander(driver, args);
    if (driver.help) {
      jCommander.usage();
      return;
    }
    driver.run(
        (project, clientParamsMap) ->
            GCEController.newGCEController(
                project,
                ImmutableMap.of(driver.zone, clientParamsMap),
                driver.cores,
                Executors.newScheduledThreadPool(500)));
  }

  public void run(BiFunction<String, Map<ClientParams, Integer>, Controller> controllerFunction) {
    try {
      Map<ClientParams, Integer> clientParamsMap = new HashMap<>();
      if (cpsGcloudJavaPublisherCount > 0) {
        clientParamsMap.put(
            new ClientParams(ClientType.CPS_GCLOUD_JAVA_PUBLISHER, null),
            cpsGcloudJavaPublisherCount);
      }
      if (cpsGcloudPythonPublisherCount > 0) {
        clientParamsMap.put(
            new ClientParams(ClientType.CPS_GCLOUD_PYTHON_PUBLISHER, null),
            cpsGcloudPythonPublisherCount);
      }
      if (cpsExperimentalJavaPublisherCount > 0) {
        clientParamsMap.put(
            new ClientParams(ClientType.CPS_EXPERIMENTAL_JAVA_PUBLISHER, null),
            cpsExperimentalJavaPublisherCount);
      }
      if (cpsVtkJavaPublisherCount > 0) {
        clientParamsMap.put(
            new ClientParams(ClientType.CPS_VTK_JAVA_PUBLISHER, null), cpsVtkJavaPublisherCount);
      }
      if (kafkaPublisherCount > 0) {
        clientParamsMap.put(
            new ClientParams(ClientType.KAFKA_PUBLISHER, null), kafkaPublisherCount);
      }
      if (kafkaSubscriberCount > 0) {
        Preconditions.checkArgument(
            kafkaPublisherCount > 0, "--kafka_publisher_count must be > 0.");
        clientParamsMap.put(
            new ClientParams(ClientType.KAFKA_SUBSCRIBER, null), kafkaSubscriberCount);
      }
      Preconditions.checkArgument(
          Durations.toMillis(publishBatchDuration) >= 0,
          "--publish_batch_duration must be positive.");
      Preconditions.checkArgument(
          !clientParamsMap.keySet().isEmpty(),
          "You must set at least one type of publisher greater than 0.");
      Preconditions.checkArgument(
          broker != null || (kafkaPublisherCount == 0 && kafkaSubscriberCount == 0),
          "If using Kafka you must provide the network address of your broker using the"
              + "--broker flag.");

      if (maxPublishLatencyTest) {
        Preconditions.checkArgument(
            clientParamsMap.size() == 1,
            "If max_publish_latency is specified, there must be one type of publisher.");
      }
      // Each type of client will have its own topic, so each topic will get
      // cpsSubscriberCount subscribers cumulatively among each of the subscriptions.
      for (int i = 0; i < cpsSubscriptionFanout; ++i) {
        if (cpsGcloudJavaSubscriberCount > 0) {
          Preconditions.checkArgument(
              cpsGcloudJavaPublisherCount + cpsGcloudPythonPublisherCount + cpsVtkJavaPublisherCount
                  > 0,
              "--cps_gcloud_java_publisher or --cps_gcloud_python_publisher must be > 0.");
          clientParamsMap.put(
              new ClientParams(ClientType.CPS_GCLOUD_JAVA_SUBSCRIBER, "gcloud-subscription" + i),
              cpsGcloudJavaSubscriberCount / cpsSubscriptionFanout);
        }
        if (cpsExperimentalJavaSubscriberCount > 0) {
          Preconditions.checkArgument(
              cpsExperimentalJavaPublisherCount > 0,
              "--cps_experimental_java_publisher or --cps_vtk_java_publisher must be > 0.");
          clientParamsMap.put(
              new ClientParams(
                  ClientType.CPS_EXPERIMENTAL_JAVA_SUBSCRIBER, "experimental-subscription" + i),
              cpsExperimentalJavaSubscriberCount / cpsSubscriptionFanout);
        }
      }
      // Set static variables.
      Controller.resourceDirectory = resourceDirectory;
      Client.messageSize = messageSize;
      Client.loadtestDuration = loadtestDuration;
      Client.publishBatchSize = publishBatchSize;
      Client.maxMessagesPerPull = cpsMaxMessagesPerPull;
      Client.pollDuration = kafkaPollDuration;
      Client.broker = broker;
      Client.requestRate = requestRate;
      Client.maxOutstandingRequests = maxOutstandingRequests;
      Client.numberOfMessages = numberOfMessages;
      Client.burnInDuration = burnInDuration;
      Client.publishBatchDuration = publishBatchDuration;

      // Start a thread to poll and output results.
      ScheduledExecutorService pollingExecutor = Executors.newSingleThreadScheduledExecutor();
      if (verbose) {
        pollingExecutor.scheduleWithFixedDelay(
            () -> {
              synchronized (pollingExecutor) {
                printStats();
              }
            },
            VERBOSE_STATS_POLL_SECONDS,
            VERBOSE_STATS_POLL_SECONDS,
            TimeUnit.SECONDS);
      }
      if (maxPublishLatencyTest) {
        controller = controllerFunction.apply(project, clientParamsMap);
        runMaxPublishLatencyTest();
      } else if (maxSubscriberThroughputTest) {
        controller = controllerFunction.apply(project, clientParamsMap);
        runMaxSubscriberThroughputTest();
      } else if (numCoresTest) {
        runNumCoresTest(clientParamsMap, controllerFunction);
      } else {
        controller = controllerFunction.apply(project, clientParamsMap);
        Map<ClientType, Controller.LoadtestStats> statsMap = runTest(null);
        GnuPlot.plot(statsMap);
        CsvOutput.outputStats(statsMap);
      }
      synchronized (pollingExecutor) {
        pollingExecutor.shutdownNow();
      }
      System.exit(0);
    } catch (Throwable t) {
      log.error("An error occurred...", t);
      System.exit(1);
    }
  }

  private Map<ClientType, Controller.LoadtestStats> runTest(Runnable whileRunning)
      throws Throwable {
    Map<ClientType, Controller.LoadtestStats> statsMap;
    Client.startTime =
        Timestamp.newBuilder().setSeconds(System.currentTimeMillis() / 1000 + 90).build();
    MessageTracker messageTracker =
        new MessageTracker(
            numberOfMessages,
            cpsGcloudJavaPublisherCount
                + cpsExperimentalJavaPublisherCount
                + cpsVtkJavaPublisherCount
                + cpsGcloudPythonPublisherCount);
    controller.startClients(messageTracker);
    if (whileRunning != null) {
      whileRunning.run();
    }
    // Wait for the load test to finish.
    controller.waitForClients();
    statsMap = controller.getStatsForAllClientTypes();
    printStats(statsMap);
    Iterable<MessageIdentifier> missing = messageTracker.getMissing();
    if (missing.iterator().hasNext()) {
      log.error("Some published messages were not received!");
      for (MessageIdentifier identifier : missing) {
        log.error(
            String.format(
                "%d:%d", identifier.getPublisherClientId(), identifier.getSequenceNumber()));
      }
    }
    if (spreadsheetId.length() > 0) {
      // Output results to common Google sheet
      new SheetsService(dataStoreDirectory, controller.getTypes())
          .sendToSheets(spreadsheetId, statsMap);
    }
    return statsMap;
  }

  private void runMaxPublishLatencyTest() throws Throwable {
    int highestRequestRate = 0;
    for (AtomicDouble publishLatency = new AtomicDouble(0);
        publishLatency.get() < maxPublishLatencyMillis;
        Client.requestRate = (int) (Client.requestRate * 1.1)) {
      Map<ClientType, Controller.LoadtestStats> statsMap = runTest(null);
      statsMap.forEach(
          (type, stats) -> {
            if (type.isPublisher()) {
              publishLatency.set(
                  LatencyDistribution.getNthPercentileUpperBound(
                      stats.bucketValues, maxPublishLatencyPercentile));
            }
          });
      if (publishLatency.get() < maxPublishLatencyMillis) {
        highestRequestRate = Client.requestRate;
      }
    }
    log.info("Maximum Request Rate: " + highestRequestRate);
    controller.shutdown(null);
  }

  private void runMaxSubscriberThroughputTest() throws Throwable {
    HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = new JacksonFactory();
    GoogleCredential credential =
        GoogleCredential.getApplicationDefault(transport, jsonFactory)
            .createScoped(
                Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    Monitoring monitoring =
        new Monitoring.Builder(transport, jsonFactory, credential)
            .setApplicationName("Cloud Pub/Sub Loadtest Framework")
            .build();
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    dateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
    for (AtomicLong backlogSize = new AtomicLong(0);
        backlogSize.get() < maxSubscriberThroughputTestBacklog;
        Client.requestRate = (int) (Client.requestRate * 1.1)) {
      runTest(
          () -> {
            try {
              Date startDate = new Date();
              startDate.setTime(Timestamps.toMillis(Client.startTime));
              controller.waitForPublisherClients();
              ListTimeSeriesResponse response =
                  monitoring
                      .projects()
                      .timeSeries()
                      .list("projects/" + project)
                      .setFilter(
                          "metric.type = "
                              + "\"pubsub.googleapis.com/subscription/num_undelivered_messages\"")
                      .setIntervalStartTime(dateFormatter.format(startDate))
                      .setIntervalEndTime(dateFormatter.format(new Date()))
                      .execute();
              // Get most recent point.
              Point latestBacklogSize = null;
              for (TimeSeries timeSeries : response.getTimeSeries()) {
                for (Point point : timeSeries.getPoints()) {
                  if (latestBacklogSize == null
                      || dateFormatter
                          .parse(point.getInterval().getStartTime())
                          .after(
                              dateFormatter.parse(
                                  latestBacklogSize.getInterval().getStartTime()))) {
                    latestBacklogSize = point;
                  }
                }
              }
              if (latestBacklogSize != null) {
                backlogSize.set(latestBacklogSize.getValue().getInt64Value());
              }
            } catch (Throwable t) {
              throw new RuntimeException("Error getting backlog stats.", t);
            }
          });
    }
    log.info(
        "We accumulated a backlog during this test, refer to the last run "
            + "for the maximum throughput attained before accumulating backlog.");
    controller.shutdown(null);
  }

  private void runNumCoresTest(
      Map<ClientParams, Integer> clientParamsMap,
      BiFunction<String, Map<ClientParams, Integer>, Controller> controllerFunction)
      throws Throwable {
    Map<ClientType, Controller.LoadtestStats> statsMap;
    GnuPlot gnuPlot = new GnuPlot();
    CsvOutput csv = new CsvOutput();
    for (cores = 1; cores <= 16; cores *= 2) {
      controller = controllerFunction.apply(project, clientParamsMap);
      statsMap = runTest(null);
      gnuPlot.addCoresResult(cores, statsMap);
      csv.addCoresResult(cores, statsMap);
      controller.shutdown(null);
    }
    gnuPlot.plotStatsPerCore();
    csv.outputStatsPerCore();
  }

  private void printStats() {
    printStats(controller.getStatsForAllClientTypes());
  }

  private void printStats(Map<ClientType, Controller.LoadtestStats> results) {
    long startMillis = Timestamps.toMillis(Timestamps.add(Client.startTime, Client.burnInDuration));
    if (System.currentTimeMillis() < startMillis) {
      log.info(
          "Still under burn in time, will start in "
              + ((startMillis - System.currentTimeMillis()) / 1000)
              + " seconds.");
      return;
    }
    log.info("===============================================");
    results.forEach(
        (type, stats) -> {
          log.info("Results for " + type + ":");
          log.info("50%: " + LatencyDistribution.getNthPercentile(stats.bucketValues, 50.0));
          log.info("99%: " + LatencyDistribution.getNthPercentile(stats.bucketValues, 99.0));
          log.info("99.9%: " + LatencyDistribution.getNthPercentile(stats.bucketValues, 99.9));
          log.info(
              "Average throughput: "
                  + new DecimalFormat("#.##").format(stats.getQPS() * messageSize / 1000000.0)
                  + " MB/s");
        });
    log.info("===============================================");
  }

  /**
   * A validator that makes sure the parameter is an integer that is greater than 0.
   *
   * @throws ParameterException The field is not a number greater than 0.
   */
  public static class GreaterThanZeroValidator implements IParameterValidator {
    @Override
    public void validate(String name, String value) {
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

  /**
   * A validator that makes sure cores is set to a valid value.
   *
   * @throws ParameterException The cores field is set to an illegal value.
   */
  public static class CoresValidator implements IParameterValidator {
    @Override
    public void validate(String name, String value) {
      try {
        int n = Integer.parseInt(value);
        if (n != 1 && n != 2 && n != 4 && n != 8 && n != 16) {
          throw new ParameterException(
              "Parameter " + name + " should be 1, 2, 4, 8, or 16 (found " + value + ")");
        }
      } catch (NumberFormatException e) {
        throw new ParameterException(
            "Parameter " + name + " should be 1, 2, 4, 8, or 16 (found " + value + ")");
      }
    }
  }

  /** A converter from {@link String} to {@link com.google.protobuf.Duration}. */
  public static class DurationConverter extends BaseConverter<Duration> {

    public DurationConverter(String optionName) {
      super(optionName);
    }

    private static int millisPerUnit(String unit) {
      switch (Ascii.toLowerCase(unit)) {
        case "d":
          return DateTimeConstants.MILLIS_PER_DAY;
        case "h":
          return DateTimeConstants.MILLIS_PER_HOUR;
        case "m":
          return DateTimeConstants.MILLIS_PER_MINUTE;
        case "s":
          return DateTimeConstants.MILLIS_PER_SECOND;
        case "ms":
          return 1;
        default:
          throw new IllegalArgumentException("Unknown duration unit " + unit);
      }
    }

    @Override
    public Duration convert(String value) {
      try {
        if (value.isEmpty()) {
          return Durations.fromMillis(0);
        }
        long millis = 0;
        boolean negative = value.startsWith("-");
        int index = negative ? 1 : 0;
        Pattern unitPattern =
            Pattern.compile(
                "(?x) (?<whole>[0-9]+)? (?<frac>\\.[0-9]*)? (?<unit>d|h|ms?|s)",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = unitPattern.matcher(value);
        while (matcher.find(index) && matcher.start() == index) {
          Preconditions.checkArgument(CharMatcher.inRange('0', '9').matchesAnyOf(matcher.group(0)));
          long whole = Long.parseLong(MoreObjects.firstNonNull(matcher.group("whole"), "0"));
          double frac =
              Double.parseDouble("0" + MoreObjects.firstNonNull(matcher.group("frac"), ""));
          int millisPerUnit = millisPerUnit(matcher.group("unit"));
          millis += millisPerUnit * whole;
          millis += (long) (millisPerUnit * frac);
          index = matcher.end();
        }
        if (index < value.length()) {
          throw new IllegalArgumentException("Could not parse entire duration");
        }
        if (negative) {
          millis = -millis;
        }
        return Durations.fromMillis(millis);
      } catch (Exception e) {
        throw new ParameterException(
            getErrorString(value, "A duration string must include units (d|h|m|s|ms)."));
      }
    }
  }
}
