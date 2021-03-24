package com.google.cloud.pubsub.sql.providers;

import com.google.cloud.pubsub.sql.Rows;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Produces a row conforming to the standard row definition.
 */
public interface StandardSource {

  Schema nativeSchema();

  /**
   * Transform from the native schema to the standard schema.
   */
  PTransform<PCollection<Row>, PCollection<Row>> transform();
}
