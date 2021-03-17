package com.google.cloud.pubsub.sql.providers;

import com.google.cloud.pubsub.sql.MakePtransform;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public interface StandardSqlSource extends StandardSource {

  /**
   * A statement transforming from the native schema to the standard schema if needed. The provided
   * table is PCOLLECTION.
   */
  String query();

  @Override
  default PTransform<PCollection<Row>, PCollection<Row>> transform() {
    return MakePtransform
        .from(rows -> rows.apply(SqlTransform.query(query())), "StandardSqlSink Transform");
  }
}
