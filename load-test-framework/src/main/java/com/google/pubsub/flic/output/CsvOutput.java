/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.google.pubsub.flic.output;

import com.google.pubsub.flic.common.StatsUtils;
import com.google.pubsub.flic.controllers.Client;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Outputs load test results to a CSV file. */
public class CsvOutput implements ResultsOutput {
  private static final Logger log = LoggerFactory.getLogger(CsvOutput.class);
  private static final String CSV_HEADER =
      "Messaging Type,Language,Messaging Side,Num Clients,Cores Per Client,Message Size,CPU Scaling,QPS,Throughput (MB/s),50%ile Latency,90%ile Latency,99%ile Latency,"
          + "99.9%ile Latency\n";
  private static final DecimalFormat decimalFormat = new DecimalFormat("#.##");

  public CsvOutput() {}

  private static String buildRow(TrackedResult result) {
    int numClients =
        result.type.isPublisher()
            ? result.testParameters.numPublisherWorkers()
            : result.testParameters.numSubscriberWorkers();

    int rawCpuScaling =
        result.type.isPublisher()
            ? Client.PUBLISHER_CPU_SCALING
            : result.testParameters.subscriberCpuScaling();
    String cpuScaling;
    switch (result.type.language) {
      case JAVA:
      case GO:
      case DOTNET:
        cpuScaling = Integer.toString(rawCpuScaling);
        break;
      case PYTHON:
      case NODE:
        cpuScaling = "NA";
        break;
      default:
        throw new RuntimeException("Language not handled by csv output: " + result.type.language);
    }

    return String.join(
        ",",
        result.type.messaging.name(),
        result.type.language.name(),
        result.type.side.name(),
        Integer.toString(numClients),
        Integer.toString(result.testParameters.numCoresPerWorker()),
        Integer.toString(result.testParameters.messageSize()),
        cpuScaling,
        decimalFormat.format(
            StatsUtils.getQPS(result.tracker.getCount(), result.testParameters.loadtestDuration())),
        decimalFormat.format(
            StatsUtils.getThroughput(
                result.tracker.getCount(),
                result.testParameters.loadtestDuration(),
                result.testParameters.messageSize())),
        result.tracker.getNthPercentileMidpoint(50),
        result.tracker.getNthPercentileMidpoint(90),
        result.tracker.getNthPercentileMidpoint(99),
        result.tracker.getNthPercentileMidpoint(99.9));
  }

  private static String buildCsv(List<TrackedResult> results) {
    return CSV_HEADER + results.stream().map(CsvOutput::buildRow).collect(Collectors.joining("\n"));
  }

  @Override
  public void outputStats(List<TrackedResult> results) {
    try (Writer writer =
        new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream("output.csv"), StandardCharsets.UTF_8))) {
      writer.write(buildCsv(results));
    } catch (IOException e) {
      log.error("Error writing CSV.", e);
    }
  }
}
