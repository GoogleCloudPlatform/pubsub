package com.google.cloud.pubsub.sql.providers;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

@AutoService({StandardSourceProvider.class, StandardSinkProvider.class})
public class CloudPubsubProvider implements StandardSourceProvider, StandardSinkProvider {
  private CloudPubsubProvider() {}

  static final Schema ATTRIBUTE_ARRAY_ENTRY_SCHEMA =
      Schema.builder().addStringField("key").addStringField("value").build();
  static final Schema.FieldType ATTRIBUTE_ARRAY_FIELD_TYPE =
      Schema.FieldType.array(Schema.FieldType.row(ATTRIBUTE_ARRAY_ENTRY_SCHEMA));
  static final Schema SCHEMA =
      Schema.builder()
          .addByteArrayField("payload")
          .addField("attributes", ATTRIBUTE_ARRAY_FIELD_TYPE)
          .addField("event_timestamp", FieldType.DATETIME.withNullable(true))
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
        return String.join(
            "\n",
            "SELECT event_timestamp, payload, ARRAY(",
            "  SELECT kv.key AS key, CAST(STRING_AGG(kv.values, \"|\") AS STRING) AS value",
            "  FROM UNNEST(attributes) AS kv) as attributes");
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
        return String.join(
            "\n",
            "SELECT event_timestamp, payload, ARRAY(",
            "  SELECT kv.key AS key, [CAST(kv.value AS BYTES)] AS values",
            "  FROM UNNEST(attributes) AS kv) as attributes");
      }
    };
  }

  @Override
  public String identifier() {
    return "pubsub";
  }
}
