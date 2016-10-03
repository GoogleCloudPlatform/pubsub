package com.google.pubsub.flic.processing;

import java.util.concurrent.atomic.AtomicInteger;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

public class StatAggregator {
  
  // Total number of messages processed.
  private int messageNo;
  // Total size of messages processed. 
  private int sizeReceived;
  private Histogram latencyStats;
  // Keeps track of the earliest and last received messages timestamps, used to find throughput 
  private long firstReceived;
  private long lastReceived;
  
  public StatAggregator() {
    messageNo = 0;
    latencyStats = new ConcurrentHistogram(1, 10000000, 2);
    firstReceived = Long.MAX_VALUE;
    lastReceived = Long.MIN_VALUE;
    sizeReceived = 0;
  }
}

