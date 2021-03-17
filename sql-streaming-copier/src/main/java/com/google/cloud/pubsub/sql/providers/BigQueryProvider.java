package com.google.cloud.pubsub.sql.providers;

import com.google.auto.service.AutoService;
import com.google.cloud.pubsub.sql.Rows;
import org.apache.beam.sdk.schemas.Schema;

@AutoService(StandardSinkProvider.class)
public class BigQueryProvider implements StandardSinkProvider {

  @Override
  public StandardSink getSink() {
    return new StandardSqlSink() {

      @Override
      public Schema nativeSchema() {
        return Rows.STANDARD_SCHEMA;
      }

      @Override
      public String query() {
        return "SELECT * FROM PCOLLECTION";
      }
    };
  }

  @Override
  public String identifier() {
    return "bigquery";
  }
}
