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
import com.google.pubsub.flic.argumentparsing.BaseArguments;
import com.google.pubsub.flic.argumentparsing.CPSArguments;
import com.google.pubsub.flic.argumentparsing.CompareArguments;
import com.google.pubsub.flic.argumentparsing.KafkaArguments;
import com.google.pubsub.flic.common.Utils;
import com.google.pubsub.flic.cps.CPSPublishingTask;
import com.google.pubsub.flic.cps.CPSRoundRobinPublisher;
import com.google.pubsub.flic.cps.CPSRoundRobinSubscriber;
import com.google.pubsub.flic.cps.CPSSubscribingTask;
import com.google.pubsub.flic.kafka.KafkaConsumerTask;
import com.google.pubsub.flic.kafka.KafkaPublishingTask;
import com.google.pubsub.flic.processing.Comparison;
import com.google.pubsub.flic.processing.MessageProcessingHandler;
import com.google.pubsub.flic.task.TaskArgs;
import io.grpc.Status;
import java.io.File;
import java.util.logging.LogManager;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Drives the execution of the framework through command line arguments. */
public class Driver {

  private static final Logger log = LoggerFactory.getLogger(Driver.class.getName());

  public static void main(String[] args) {
    // Turns off all java.util.logging.
    LogManager.getLogManager().reset();
    try {
      // Parse command line arguments.
      BaseArguments baseArgs = new BaseArguments();
      CPSArguments cpsArgs = new CPSArguments();
      KafkaArguments kafkaArgs = new KafkaArguments();
      CompareArguments dataComparisonArgs = new CompareArguments();
      JCommander jCommander = new JCommander(baseArgs);
      jCommander.addCommand(CPSArguments.COMMAND, cpsArgs);
      jCommander.addCommand(KafkaArguments.COMMAND, kafkaArgs);
      jCommander.addCommand(CompareArguments.COMMAND, dataComparisonArgs);
      jCommander.parse(args);
      if (jCommander.getParsedCommand() == null) {
        if (baseArgs.isHelp()) {
          jCommander.usage();
        }
        return;
      }
      if (jCommander.getParsedCommand().equals(CompareArguments.COMMAND)) {
        // Compares data dumps for correctness.
        Comparison c = new Comparison(dataComparisonArgs.getFile1(), dataComparisonArgs.getFile2());
        c.compare();
        return;
      }
      // Use the builder to construct custom arguments for each task.
      TaskArgs.TaskArgsBuilder builder =
          new TaskArgs.TaskArgsBuilder()
              .numMessages(baseArgs.getNumMessages())
              .topics(baseArgs.getTopics());
      TaskArgs taskArgs;
      if (jCommander.getParsedCommand().equals(CPSArguments.COMMAND)) {
        // The "cps" command was invoked.
        MessageProcessingHandler cpsHandler =
            new MessageProcessingHandler(baseArgs.getNumMessages());
        builder =
            builder
                .cpsProject(cpsArgs.getProject())
                .numResponseThreads(cpsArgs.getNumResponseThreads())
                .rateLimit(cpsArgs.getRateLimit());
        if (baseArgs.isPublish()) {
          // Create a task which publishes to CPS.
          taskArgs =
              builder
                  .messageSize(baseArgs.getMessageSize())
                  .batchSize(cpsArgs.getBatchSize())
                  .build();
          cpsHandler.setLatencyType(MessageProcessingHandler.LatencyType.PUB_TO_ACK);
          CPSRoundRobinPublisher publisher = new CPSRoundRobinPublisher(cpsArgs.getNumClients());
          log.info("Creating a task which publishes to CPS.");
          new CPSPublishingTask(taskArgs, publisher, cpsHandler).execute();
        } else {
          // Create a task which consumes from CPS.
          if (baseArgs.isDumpData()) {
            cpsHandler.setFiledump(new File(Utils.CPS_FILEDUMP_PATH));
          }
          taskArgs = builder.build();
          cpsHandler.setLatencyType(MessageProcessingHandler.LatencyType.END_TO_END);
          CPSRoundRobinSubscriber subscriber = new CPSRoundRobinSubscriber(cpsArgs.getNumClients());
          log.info("Creating a task which consumes from CPS.");
          try {
            new CPSSubscribingTask(taskArgs, subscriber, cpsHandler).execute();
          } catch(io.grpc.StatusRuntimeException e) {
            // Catch error for already existing subscription, then creates a client
            if(e.getStatus().getCode() == Status.ALREADY_EXISTS.getCode())  {
              System.out.println("hi");
            }
            else
              throw e;
          }
        }
      } else {
        // The "kafka" command was invoked.
        MessageProcessingHandler bundle = new MessageProcessingHandler(baseArgs.getNumMessages());
        builder = builder.broker(kafkaArgs.getBroker());
        if (baseArgs.isPublish()) {
          // Create a task that publishes to Kafka.
          taskArgs = builder.messageSize(baseArgs.getMessageSize()).build();
          KafkaProducer<String, String> publisher =
              KafkaPublishingTask.getInitializedProducer(taskArgs);
          log.info("Creating a task which publishes to Kafka");
          new KafkaPublishingTask(taskArgs, publisher).execute();
        } else {
          // Create a task that consumes from Kafka.
          if (baseArgs.isDumpData()) {
            bundle.setFiledump(new File(Utils.KAFKA_FILEDUMP_PATH));
          }
          taskArgs = builder.broker(kafkaArgs.getBroker()).build();
          KafkaConsumer<String, String> consumer =
              KafkaConsumerTask.getInitializedConsumer(taskArgs);
          log.info("Creating a task which consumes from Kafka.");
          new KafkaConsumerTask(taskArgs, consumer, bundle).execute();
        }
      }
    } catch (Exception e) {
      log.error("An error occurred...", e);
      System.exit(1);
    }
  }
}
