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
import com.google.pubsub.flic.controllers.Client.ClientType;
import com.google.pubsub.flic.controllers.GCEController;
import com.google.pubsub.flic.processing.Comparison;
import com.google.pubsub.flic.task.TaskArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.LogManager;

/**
 * Drives the execution of the framework through command line arguments.
 */
public class Driver {

  private static final Logger log = LoggerFactory.getLogger(Driver.class);

  public static void main(String[] args) {
    // Turns off all java.util.logging.
    LogManager.getLogManager().reset();
    try {
      // Parse command line arguments.
      BaseArguments baseArgs = new BaseArguments();
      CPSArguments cpsArgs = new CPSArguments();
      KafkaArguments kafkaArgs = new KafkaArguments();
      CompareArguments dataComparisonArgs = new CompareArguments();
      JCommander jCommander = new JCommander(BaseArguments.class);
      jCommander.addCommand(CPSArguments.COMMAND, cpsArgs);
      jCommander.addCommand(KafkaArguments.COMMAND, kafkaArgs);
      jCommander.addCommand(CompareArguments.COMMAND, dataComparisonArgs);
      jCommander.parse(args);
      if (jCommander.getParsedCommand() == null) {
        if (baseArgs.isHelp()) {
          jCommander.usage();
          return;
        }
        System.exit(1);
      }
      // Compares data dumps for correctness.
      if (jCommander.getParsedCommand().equals(CompareArguments.COMMAND)) {
        Comparison c = new Comparison(dataComparisonArgs.getFile1(), dataComparisonArgs.getFile2());
        c.compare();
        return;
      }

      Map<ClientType, Integer> clientTypes = baseArgs.getClientTypes();
      if (clientTypes.values().stream().allMatch((n) -> n == 0)) {
        jCommander.usage();
        return;
      }

      TaskArgs taskArgs;
      GCEController controller = GCEController.newGCEController(baseArgs.getProject(),
          clientTypes, Executors.newCachedThreadPool());
    } catch (Exception e) {
      log.error("An error occurred...", e);
      System.exit(1);
    }
  }
}
