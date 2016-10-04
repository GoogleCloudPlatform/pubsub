package com.google.pubsub.flic.processing;

import com.google.protobuf.Parser;
import com.google.pubsub.flic.common.MessagePacketProto.MessagePacket;
import com.google.pubsub.flic.processing.MessageProcessingHandler.LatencyType;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatAggregator {
  
  private static final Logger log =
      LoggerFactory.getLogger(StatAggregator.class.getName());
  
  // Total number of messages processed.
  private int messageNo;
  // Total size of messages processed. 
  private int sizeReceived;
  private Histogram latencyStats;
  // Keeps track of the earliest and last received messages timestamps, used to find throughput 
  private long firstReceived;
  private long lastReceived;
  private List<File> files;
  
  public StatAggregator(List<String> filenames) {
    messageNo = 0;
    latencyStats = new ConcurrentHistogram(1, 10000000, 2);
    firstReceived = Long.MAX_VALUE;
    lastReceived = Long.MIN_VALUE;
    sizeReceived = 0;
    files = new ArrayList<File>();
    File currentDirectory = new File(".");
    for (String name : filenames) {
      // Add all files that match the pattern given by name
      FileFilter filter = new WildcardFileFilter(name);
      files.addAll(Arrays.asList(currentDirectory.listFiles(filter)));
    }
  }
  
  public void generateStats() throws Exception {
    Parser<MessagePacket> parser = MessagePacket.PARSER;
    for (File f : files) { 
      InputStream is = new FileInputStream(f);
      MessagePacket mp = parser.parseDelimitedFrom(is);
      while(mp != null) {
        messageNo++;
        sizeReceived+= mp.getValue().getBytes("UTF-8").length;
        long receivedTime = mp.getReceivedTime();
        if (receivedTime < firstReceived) {
          firstReceived = receivedTime;
        }
        if (receivedTime > lastReceived) {
          lastReceived = receivedTime;
        }
        latencyStats.recordValue(mp.getLatency());
        mp = parser.parseDelimitedFrom(is);
      } 
    }
  }
  
  public void printStats() {
    // Latency stats.
    log.info(
        LatencyType.END_TO_END
            + " for "
            + latencyStats.getTotalCount()
            + " messages: "
            + String.format(
                MessageProcessingHandler.LATENCY_STATS_FORMAT,
                latencyStats.getMinValue(),
                latencyStats.getMaxValue(),
                latencyStats.getMean(),
                latencyStats.getValueAtPercentile(50),
                latencyStats.getValueAtPercentile(95),
                latencyStats.getValueAtPercentile(99)));
    // Throughput stats.
    double diff = (double) (lastReceived - firstReceived) / 1000;
    if(diff != 0) {
      long averageMessagesPerSec = (long) (messageNo / diff);
      long averageBytesPerSec = (long) (sizeReceived / diff);
      String averageBytesPerSecString = FileUtils.byteCountToDisplaySize(averageBytesPerSec);
      log.info(
          "Average throughput: "
              + averageMessagesPerSec
              + " messages/sec, "
              + averageBytesPerSecString
              + "/sec");
    } else {
      log.info( "All messages received in one batch so receive throughput can't be calculated.");
      log.info("Total messages: "
              + messageNo 
              + ". Total bytes: "
              + sizeReceived
              + ". Bytes per message: "
              + (sizeReceived / messageNo)
              + ".");
    }
  }
}

