package com.google.cloud.pubsub.sql.providers;

import com.google.auto.service.AutoService;
import com.google.cloud.pubsub.sql.Rows;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

@AutoService({StandardSourceProvider.class, StandardSinkProvider.class})
public class PubsubLiteProvider implements StandardSourceProvider, StandardSinkProvider {

  @Override
  public StandardSink getSink() {
    return (StandardSqlSink) () -> Rows.STANDARD_SCHEMA;
  }

  private static final Schema READ_SCHEMA = Schema.builder()
      .addByteArrayField(Rows.PAYLOAD_FIELD)
      .addByteArrayField(Rows.MESSAGE_KEY_FIELD)
      .addDateTimeField("publish_timestamp")
      .addField(Rows.EVENT_TIMESTAMP_FIELD, FieldType.DATETIME.withNullable(true))
      .addArrayField(Rows.ATTRIBUTES_FIELD, Schema.FieldType.row(Rows.ATTRIBUTES_ENTRY_SCHEMA))
      .build();

  @Override
  public StandardSource getSource() {
    return new StandardSqlSource() {
      @Override
      public String query() {
        return "SELECT payload, message_key, attributes, IFNULL(event_timestamp, publish_timestamp) AS event_timestamp FROM PCOLLECTION";
      }

      @Override
      public Schema nativeSchema() {
        return READ_SCHEMA;
      }
    };
  }

  @Override
  public String identifier() {
    return "pubsublite";
  }
}
