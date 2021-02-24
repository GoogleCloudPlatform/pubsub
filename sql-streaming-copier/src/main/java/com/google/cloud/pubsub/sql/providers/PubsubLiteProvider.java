package com.google.cloud.pubsub.sql.providers;

import com.google.auto.service.AutoService;

@AutoService({StandardSourceProvider.class, StandardSinkProvider.class})
public class PubsubLiteProvider implements StandardSourceProvider, StandardSinkProvider {
  @Override
  public StandardSqlSink getSink() {
    return new StandardSqlSink() {}; // Pub/Sub lite uses the standard schema.
  }

  @Override
  public StandardSqlSource getSource() {
    return new StandardSqlSource() {}; // Pub/Sub lite uses the standard schema.
  }

  @Override
  public String identifier() {
    return "pubsublite";
  }
}
