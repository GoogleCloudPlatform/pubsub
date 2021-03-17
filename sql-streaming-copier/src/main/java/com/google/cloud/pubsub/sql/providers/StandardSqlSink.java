package com.google.cloud.pubsub.sql.providers;

import com.google.cloud.pubsub.sql.MakePtransform;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public interface StandardSqlSink extends StandardSink {

  /**
   * A statement transforming from the standard schema to the native schema if needed. The empty
   * string if no statement is required. The provided table is PCOLLECTION.
   */
  default String query() {
    return "";
  }

  @Override
  default PTransform<PCollection<Row>, PCollection<Row>> transform() {
    return MakePtransform.from(rows -> {
      if (!query().isEmpty()) {
        rows = rows.apply(SqlTransform.query(query()));
      }
      return rows;
    }, "StandardSqlSink Transform");
  }
}
