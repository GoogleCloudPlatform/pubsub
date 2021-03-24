package com.google.cloud.pubsub.sql.providers;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Consumes rows conforming to the standard row definition.
 */
public interface StandardSink {

  Schema nativeSchema();

  /**
   * Transform from the standard schema to the native schema.
   */
  PTransform<PCollection<Row>, PCollection<Row>> transform();
}
