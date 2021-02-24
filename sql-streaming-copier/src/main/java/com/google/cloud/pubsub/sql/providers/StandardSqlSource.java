package com.google.cloud.pubsub.sql.providers;

import com.google.cloud.pubsub.sql.Rows;
import org.apache.beam.sdk.schemas.Schema;

/** Produces a row conforming to the standard row definition. */
public interface StandardSqlSource {
  default Schema nativeSchema() {
    return Rows.STANDARD_SCHEMA;
  }
  /**
   * A statement transforming to the standard schema from the native schema if needed. The empty
   * string if no statement is required.
   */
  default String selectStatement() {
    return "";
  }
}
