package com.google.cloud.pubsub.sql.providers;

import static com.google.cloud.pubsub.sql.Rows.ATTRIBUTES_FIELD_TYPE;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

@AutoService({StandardSourceProvider.class, StandardSinkProvider.class})
public class KafkaProvider implements StandardSourceProvider, StandardSinkProvider {

  static final Schema SCHEMA =
      Schema.builder()
          .addByteArrayField("payload")
          .addField("headers", ATTRIBUTES_FIELD_TYPE)
          .addField("event_timestamp", FieldType.DATETIME.withNullable(true))
          .addField("message_key", FieldType.BYTES.withNullable(true))
          .build();

  @Override
  public StandardSink getSink() {
    return new StandardSqlSink() {
      @Override
      public Schema nativeSchema() {
        return SCHEMA;
      }

      @Override
      public String query() {
        // Beam does not support null keys (https://issues.apache.org/jira/browse/BEAM-12008)
        // So generate a random float and convert it to a string to get random routing for unkeyed
        // messages (all messages with empty keys route to the same partition).
        return "SELECT event_timestamp, payload, attributes AS headers, IF(message_key = b'', CAST(CAST(RAND() AS STRING) AS BYTES), message_key) AS message_key FROM PCOLLECTION";
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
