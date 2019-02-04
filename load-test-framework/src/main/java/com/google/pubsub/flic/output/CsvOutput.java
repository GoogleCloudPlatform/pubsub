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

package com.google.pubsub.flic.output;

import com.google.pubsub.flic.common.LatencyTracker;
import com.google.pubsub.flic.common.StatsUtils;
import com.google.pubsub.flic.controllers.Client;
import com.google.pubsub.flic.controllers.Client.ClientType;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Outputs load test results to a CSV file. */
public class CsvOutput {
  private static final Logger log = LoggerFactory.getLogger(CsvOutput.class);
  private static final String CSV_HEADER =
      "Client Type,QPS,Throughput (MB/s),50%ile Latency,90%ile Latency,99%ile Latency,"
    + "99.9%ile Latency\n";
  private static final String CSV_CORES_HEADER = "Number of Cores," + CSV_HEADER;
  private static final DecimalFormat decimalFormat = new DecimalFormat("#.##");

  private final StringBuilder coresBuilder;
  public CsvOutput() {
    coresBuilder = new StringBuilder(CSV_CORES_HEADER);
  }

  public void addCoresResult(int numCores, Map<ClientType, LatencyTracker> trackers) {
    trackers.entrySet().stream().map(kv -> buildRow(kv.getKey(), kv.getValue())).forEach(
        s -> coresBuilder.append(numCores).append(',').append(s).append('\n'));
  }

  public void outputStatsPerCore() {
    try (Writer writer =
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream("output.csv"), "utf-8"))) {
      writer.write(coresBuilder.toString());
    } catch (IOException e) {
      log.error("Error writing CSV.", e);
    }
  }

  private static String buildRow(ClientType type, LatencyTracker tracker) {
    return String.join(
        ",",
        type.toString(),
        decimalFormat.format(StatsUtils.getQPS(tracker.getCount(), Client.loadtestDuration)),
        decimalFormat.format(StatsUtils.getThroughput(tracker.getCount(), Client.loadtestDuration, Client.messageSize)),
        tracker.getNthPercentileMidpoint(50),
        tracker.getNthPercentileMidpoint(90),
        tracker.getNthPercentileMidpoint(99),
        tracker.getNthPercentileMidpoint(99.9));
  }

  private static String buildCsv(Map<ClientType, LatencyTracker> trackers) {
    return CSV_HEADER + trackers.entrySet().stream()
        .map(kv -> buildRow(kv.getKey(), kv.getValue())).collect(Collectors.joining("\n"));
  }

  public static void outputStats(Map<ClientType, LatencyTracker> trackers) {
    try (Writer writer =
        new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream("output.csv"), "utf-8"))) {
      writer.write(buildCsv(trackers));
    } catch (IOException e) {
      log.error("Error writing CSV.", e);
    }
  }
}
