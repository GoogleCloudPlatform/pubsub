package com.google.pubsublite.kafka.source;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public class PubSubLiteSourceTask extends SourceTask {

  private final PollerFactory factory;
  private @Nullable
  Poller poller;

  @VisibleForTesting
  PubSubLiteSourceTask(PollerFactory factory) {
    this.factory = factory;
  }

  public PubSubLiteSourceTask() {
    this(new PollerFactoryImpl());
  }

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    if (poller != null) {
      throw new IllegalStateException("Called start when poller already exists.");
    }
    poller = factory.newPoller(props);
  }

  @Override
  public @Nullable
  List<SourceRecord> poll() {
    return poller.poll();
  }

  @Override
  public void stop() {
    if (poller == null) {
      throw new IllegalStateException("Called stop when poller doesn't exist.");
    }
    try {
      poller.close();
    } finally {
      poller = null;
    }
  }
}
