package com.google.cloud.pubsub.sql.providers;

import com.google.auto.service.AutoService;
import com.google.cloud.pubsub.sql.MakePtransform;
import com.google.cloud.pubsub.sql.Rows;
import org.apache.beam.sdk.io.gcp.pubsublite.Uuid;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

@AutoService({StandardSourceProvider.class, StandardSinkProvider.class})
public class KafkaProvider implements StandardSourceProvider, StandardSinkProvider {

  private static final String HEADERS_FIELD = "headers";

  private static final Schema SCHEMA =
      Schema.builder()
          .addByteArrayField(Rows.PAYLOAD_FIELD)
          .addField(HEADERS_FIELD, Rows.ATTRIBUTES_FIELD_TYPE)
          .addField(Rows.EVENT_TIMESTAMP_FIELD, FieldType.DATETIME.withNullable(true))
          .addField(Rows.MESSAGE_KEY_FIELD, FieldType.BYTES)
          .build();

  private static Row toKafkaRow(Row standardRow) {
    // Beam does not support null keys (https://issues.apache.org/jira/browse/BEAM-12008)
    // So generate a random uuid if the key is empty and convert it to a string to get random
    // routing for unkeyed messages (all messages with empty keys route to the same partition).
    byte[] key = standardRow.getBytes(Rows.MESSAGE_KEY_FIELD);
    if (key.length == 0) {
      key = Uuid.random().value().toByteArray();
    }
    return Row.withSchema(SCHEMA)
        .withFieldValue(Rows.PAYLOAD_FIELD, standardRow.getBytes(Rows.PAYLOAD_FIELD))
        .withFieldValue(Rows.MESSAGE_KEY_FIELD, key)
        .withFieldValue(HEADERS_FIELD, standardRow.getBaseValue(Rows.ATTRIBUTES_FIELD))
        .withFieldValue(Rows.EVENT_TIMESTAMP_FIELD,
            standardRow.getDateTime(Rows.EVENT_TIMESTAMP_FIELD))
        .build();
  }

  @Override
  public StandardSink getSink() {
    // Beam does not support null keys (https://issues.apache.org/jira/browse/BEAM-12008)
    // So generate a random float and convert it to a string to get random routing for unkeyed
    // messages (all messages with empty keys route to the same partition).
    //
    // Beam does not support RAND() and cannot generate a random key using SQL.
    return new StandardSink() {
      @Override
      public Schema nativeSchema() {
        return SCHEMA;
      }

      @Override
      public PTransform<PCollection<Row>, PCollection<Row>> transform() {
        return MakePtransform.from(rows -> rows
            .apply(MapElements.into(TypeDescriptor.of(Row.class)).via(KafkaProvider::toKafkaRow))
            .setRowSchema(SCHEMA), "Kafka Sink Transform");
      }
    };
  }

  @Override
  public StandardSource getSource() {
    return new StandardSqlSource() {
      @Override
      public Schema nativeSchema() {
        return SCHEMA;
      }

      @Override
      public String query() {
        return "SELECT event_timestamp, payload, headers AS attributes, message_key FROM PCOLLECTION";
      }
    };
  }

  @Override
  public String identifier() {
    return "kafka";
  }
}
