package com.google.cloud.pubsub.sql.providers;

import com.google.cloud.pubsub.sql.Rows;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Consumes rows conforming to the standard row definition.
 */
public interface StandardSink {

  default Schema nativeSchema() {
    return Rows.STANDARD_SCHEMA;
  }

  /**
   * Transform from the standard schema to the native schema.
   */
  PTransform<PCollection<Row>, PCollection<Row>> transform();
}
