package com.google.pubsublite.kafka.source;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.kafka.connect.source.SourceRecord;

interface Poller extends AutoCloseable {

  @Nullable
  List<SourceRecord> poll();

  void close();
}
