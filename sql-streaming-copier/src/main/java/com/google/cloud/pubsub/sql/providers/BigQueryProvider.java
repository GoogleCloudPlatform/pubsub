package com.google.cloud.pubsub.sql.providers;

import com.google.auto.service.AutoService;
import com.google.cloud.pubsub.sql.Rows;

@AutoService(StandardSinkProvider.class)
public class BigQueryProvider implements StandardSinkProvider {

  @Override
  public StandardSink getSink() {
    return (StandardSqlSink) () -> Rows.STANDARD_SCHEMA;
  }

  @Override
  public String identifier() {
    return "bigquery";
  }
}
