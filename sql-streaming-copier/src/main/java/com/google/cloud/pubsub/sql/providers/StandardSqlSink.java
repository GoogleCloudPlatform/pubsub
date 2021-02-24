package com.google.cloud.pubsub.sql.providers;

import com.google.cloud.pubsub.sql.Rows;
import org.apache.beam.sdk.schemas.Schema;

/** Consumes rows conforming to the standard row definition. */
public interface StandardSqlSink {
  default Schema nativeSchema() {
    return Rows.STANDARD_SCHEMA;
  }
  /**
   * A statement transforming from the standard schema to the native schema if needed. The empty
   * string if no statement is required.
   */
  default String selectStatement() {
    return "";
  }
}
