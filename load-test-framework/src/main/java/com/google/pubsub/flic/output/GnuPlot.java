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

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Ascii;
import com.google.pubsub.flic.common.LatencyDistribution;
import com.google.pubsub.flic.controllers.Client.ClientType;
import com.google.pubsub.flic.controllers.Controller.LoadtestStats;

/** Outputs results of the load test as a GnuPlot. */
public class GnuPlot {
  private static final Logger log = LoggerFactory.getLogger(SheetsService.class);

  private static final String STYLE_OPTS =
      "set style line 1 lc rgb '#8b1a0e' pt 1 ps 1 lt 1 lw 2 # --- red\n"
          + "set style line 2 lc rgb '#5e9c36' pt 6 ps 1 lt 1 lw 2 # --- green\n"
          + "set style line 11 lc rgb '#808080' lt 1\n"
          + "set border 3 back ls 11\n"
          + "set tics nomirror\n"
          + "set style line 12 lc rgb '#808080' lt 0 lw 1\n"
          + "set grid back ls 12\n";

  private static final String PLOT_TEMPLATE =
      "set term png\n"
          + "set autoscale\n"
          + "set logscale y 2\n"
          + "unset label\n"
          + "set xtic auto\n"
          + "set ytic auto\n"
          + "set yr [:]\n";

  private static final String THROUGHPUT_TEMPLATE =
      "set output '%1$s-throughput-%3$d.png'\n"
          + "set title '%2$s Throughput'\n"
          + "set xlabel 'Cores'\n"
          + "set ylabel 'Throughput (MB/s)'\n"
          + "set xr [0.0:16.0]\n"
          + "plot ";

  private static final String LATENCIES_TEMPLATE =
      "set output '%1$s-latencies-%3$d.png'\n"
          + "set title '%2$s Latencies'\n"
          + "set xlabel 'Percentile'\n"
          + "set ylabel 'Latency (ms)'\n"
          + "set xr [0.0:100.0]\n"
          + "plot ";

  private static final String LATENCIES_PLOT_COMMAND =
      "'%1$s_latency.dat' using 1:2 title '%1$s' with linespoints";

  private static final String THROUGHPUT_PLOT_COMMAND =
      "'%1$s_throughput.dat' using 1:2 title '%1$s' with linespoints";

  private static String buildLatenciesPlot(String prefix, Stream<ClientType> types) {
    return STYLE_OPTS
        + PLOT_TEMPLATE
        + String.format(
            LATENCIES_TEMPLATE, Ascii.toLowerCase(prefix), prefix, System.currentTimeMillis())
        + String.join(
            ", ",
            types
                .map(type -> String.format(LATENCIES_PLOT_COMMAND, type.toString()))
                .collect(Collectors.toList()));
  }

  private static String buildThroughputPlot(String prefix, Stream<ClientType> types) {
    return STYLE_OPTS
        + PLOT_TEMPLATE
        + String.format(
            THROUGHPUT_TEMPLATE, Ascii.toLowerCase(prefix), prefix, System.currentTimeMillis())
        + String.join(
            ", ",
            types
                .map(type -> String.format(THROUGHPUT_PLOT_COMMAND, type.toString()))
                .collect(Collectors.toList()));
  }

  private static String buildLatenciesDat(LoadtestStats stats) {
    StringBuilder dat = new StringBuilder();
    for (int percentile = 5; percentile < 100; percentile += 5) {
      dat.append(percentile)
          .append(" ")
          .append(LatencyDistribution.getNthPercentileMidpoint(stats.bucketValues, percentile))
          .append("\n");
    }
    dat.append("99 ")
        .append(LatencyDistribution.getNthPercentileMidpoint(stats.bucketValues, 99))
        .append("\n" + "99.9 ")
        .append(LatencyDistribution.getNthPercentileMidpoint(stats.bucketValues, 99.9))
        .append("\n");
    return dat.toString();
  }

  public static void plot(Map<ClientType, LoadtestStats> statsMap) {
    statsMap.forEach(
        (type, stats) -> {
          try (Writer writer =
              new BufferedWriter(
                  new OutputStreamWriter(new FileOutputStream(type + "_latency.dat"), "utf-8"))) {
            writer.write(buildLatenciesDat(stats));
          } catch (IOException e) {
            log.error("Error writing latencies.plot.", e);
          }
        });
    try (Writer writer =
        new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream("publish_latencies.plot"), "utf-8"))) {
      writer.write(
          buildLatenciesPlot(
              "Publish", statsMap.keySet().stream().filter(ClientType::isPublisher)));
    } catch (IOException e) {
      log.error("Error writing publish_latencies.plot.", e);
    }
    try (Writer writer =
        new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream("end_to_end_latencies.plot"), "utf-8"))) {
      writer.write(
          buildLatenciesPlot(
              "End-to-End", statsMap.keySet().stream().filter(c -> !c.isPublisher())));
    } catch (IOException e) {
      log.error("Error writing end_to_end_latencies.plot.", e);
    }
    Runtime runtime = Runtime.getRuntime();
    try {
      runtime.exec(new String[] {"gnuplot", "publish_latencies.plot"}).waitFor();
      runtime.exec(new String[] {"gnuplot", "end_to_end_latencies.plot"}).waitFor();
      statsMap.keySet().forEach(type -> {
        try {
          Files.deleteIfExists(Paths.get(type + "_latency.dat"));
        } catch (IOException e) {
          // File must not have been created successfully
        }
      });
      Files.deleteIfExists(Paths.get("publish_latencies.plot"));
      Files.deleteIfExists(Paths.get("end_to_end_latencies.plot"));
    } catch (Exception e) {
      log.error("Error running gnuplot.", e);
    }
  }

  private final Map<ClientType, Map<Integer, Double>> coreStats;
  public GnuPlot() {
    coreStats = new HashMap<>();
  }

  public void addCoresResult(Integer numCores, Map<ClientType, LoadtestStats> statsMap) {
    statsMap.forEach((type, stats) -> {
      coreStats.putIfAbsent(type, new HashMap<>());
      coreStats.get(type).put(numCores, stats.getThroughput());
    });
  }

  private String buildThroughputDat(Map<Integer, Double> throughputMap) {
    StringBuilder dat = new StringBuilder();
    DecimalFormat decimalFormat = new DecimalFormat("#.##");
    for (int cores = 1; cores <= 16; cores *= 2) {
      try {
        Double coreValue = 0D;
        if (throughputMap.containsKey(cores)) {
          coreValue = throughputMap.get(cores);
        }
        dat.append(cores)
            .append(" ")
            .append(decimalFormat.format(coreValue))
            .append("\n");
      } catch (Exception e) {
        log.error("There was a problem getting the results from one of the tests.", e);
      }
    }
    return dat.toString();
  }

  public void plotStatsPerCore() {
    coreStats.forEach(
        (type, throughputMap) -> {
          try (Writer writer =
              new BufferedWriter(
                  new OutputStreamWriter(
                      new FileOutputStream(type + "_throughput.dat"), "utf-8"))) {
            writer.write(buildThroughputDat(throughputMap));
          } catch (IOException e) {
            log.error("Error writing throughput data.", e);
          }
        });
    try (Writer writer =
        new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream("publish_throughput.plot"), "utf-8"))) {
      writer.write(
          buildThroughputPlot(
              "Publish", coreStats.keySet().stream().filter(ClientType::isPublisher)));
    } catch (IOException e) {
      log.error("Error writing publish_throughput.plot.", e);
    }
    try (Writer writer =
        new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream("subscribe_throughput.plot"), "utf-8"))) {
      writer.write(
          buildThroughputPlot(
              "Subscribe", coreStats.keySet().stream().filter(c -> !c.isPublisher())));
    } catch (IOException e) {
      log.error("Error writing subscribe_throughput.plot.", e);
    }
    Runtime runtime = Runtime.getRuntime();
    try {
      runtime.exec(new String[] {"gnuplot", "publish_throughput.plot"}).waitFor();
      runtime.exec(new String[] {"gnuplot", "subscribe_throughput.plot"}).waitFor();
      coreStats.keySet().forEach(type -> {
        try {
          Files.deleteIfExists(Paths.get(type + "_throughput.dat"));
        } catch (IOException e) {
          // File must not have been created successfully
        }
      });
      Files.deleteIfExists(Paths.get("publish_throughput.plot"));
      Files.deleteIfExists(Paths.get("subscribe_throughput.plot"));
    } catch (Exception e) {
      log.error("Error running gnuplot.", e);
    }
  }
}
