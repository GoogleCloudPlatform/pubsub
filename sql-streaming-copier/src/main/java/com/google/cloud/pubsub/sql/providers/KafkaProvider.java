package com.google.cloud.pubsub.sql.providers;

import static com.google.cloud.pubsub.sql.Rows.ATTRIBUTES_FIELD_TYPE;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

@AutoService({StandardSourceProvider.class, StandardSinkProvider.class})
public class KafkaProvider implements StandardSourceProvider, StandardSinkProvider {
  private KafkaProvider() {}

  static final Schema SCHEMA =
      Schema.builder()
          .addByteArrayField("payload")
          .addField("headers", ATTRIBUTES_FIELD_TYPE)
          .addField("event_timestamp", FieldType.DATETIME.withNullable(true))
          .addField("message_key", FieldType.BYTES.withNullable(true))
          .build();

  @Override
  public StandardSqlSink getSink() {
    return new StandardSqlSink() {
      @Override
      public Schema nativeSchema() {
        return SCHEMA;
      }

      @Override
      public String selectStatement() {
        return "SELECT event_timestamp, payload, attributes AS headers, message_key";
      }
    };
  }

  @Override
  public StandardSqlSource getSource() {
    return new StandardSqlSource() {
      @Override
      public Schema nativeSchema() {
        return SCHEMA;
      }

      @Override
      public String selectStatement() {
        return "SELECT event_timestamp, payload, headers AS attributes, message_key";
      }
    };
  }

  @Override
  public String identifier() {
    return "kafka";
  }
}
