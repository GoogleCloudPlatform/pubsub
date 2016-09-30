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
import com.google.common.collect.ImmutableMap;
import com.google.pubsub.flic.common.Utils;
import com.google.pubsub.flic.controllers.Client.ClientType;
import com.google.pubsub.flic.controllers.GCEController;
import com.google.pubsub.flic.processing.Comparison;
import com.google.pubsub.flic.task.TaskArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.LogManager;

/**
 * Drives the execution of the framework through command line arguments.
 */
public class Driver {

  public static final String COMMAND = "compare";
  private final static Logger log = LoggerFactory.getLogger(Driver.class);
  @Parameter(
      names = {"--help"},
      help = true
  )
  private boolean help = false;
  @Parameter(
      names = {"--topics", "-t"},
      required = true,
      description = "Topics to publish/consume from."
  )
  private List<String> topics = new ArrayList<>();
  @Parameter(
      names = {"--dump_data", "-d"},
      description = "Whether to dump relevant message data (only when consuming messages)."
  )
  private boolean dumpData = false;
  @Parameter(
      names = {"--cps_publisher_count"},
      description = "Number of CPS publishers to start."
  )
  private int cpsPublisherCount = 0;
  @Parameter(
      names = {"--cps_subscriber_count"},
      description = "Number of CPS subscribers to start per ."
  )
  private int cpsSubscriberCount = 0;
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
  private int messageSize = 10;
  @Parameter(
      names = {"--project", "-u"},
      required = true,
      description = "Cloud Pub/Sub project name."
  )
  private String project;
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
      names = {"--rate_limit", "-l"},
      description = "Max number of requests per second.",
      validateWith = Utils.GreaterThanZeroValidator.class
  )
  private int rateLimit = 1000;
  @Parameter(
      names = {"--file1", "-f1"},
      required = true
  )
  private String file1;
  @Parameter(
      names = {"--file2", "-f2"},
      required = true
  )
  private String file2;

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
    if (jCommander.getParsedCommand().equals(Comparison.COMMAND)) {
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
    try {
      Map<ClientType, Integer> clientTypes = ImmutableMap.of(
          ClientType.CPS_GRPC_PUBLISHER, cpsPublisherCount,
          ClientType.CPS_GRPC_SUBSCRIBER, cpsSubscriberCount,
          ClientType.KAFKA_PUBLISHER, kafkaPublisherCount,
          ClientType.KAFKA_SUBSCRIBER, kafkaSubscriberCount
      );
      if (clientTypes.values().stream().allMatch((n) -> n == 0)) {
        log.error("You must specify at least one client type to use.");
        System.exit(1);
      }

      TaskArgs taskArgs;
      GCEController controller = GCEController.newGCEController(project, clientTypes, Executors.newCachedThreadPool());
    } catch (Exception e) {
      log.error("An error occurred...", e);
      System.exit(1);
    }
  }
}
