package com.google.cloud.pubsub.sql.providers;

import com.google.auto.service.AutoService;

@AutoService(StandardSinkProvider.class)
public class BigQueryProvider implements StandardSinkProvider {
  @Override
  public StandardSqlSink getSink() {
    return new StandardSqlSink() {}; // BigQuery uses the standard schema.
  }

  @Override
  public String identifier() {
    return "bigquery";
  }
}
