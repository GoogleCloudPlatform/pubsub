package com.google.cloud.pubsub.sql;

import org.apache.beam.sdk.schemas.Schema;

public class Rows {
  private Rows() {}

  public static final String MESSAGE_KEY_FIELD = "message_key";
  public static final String EVENT_TIMESTAMP_FIELD = "event_timestamp";
  public static final String ATTRIBUTES_FIELD = "attributes";
  public static final String PAYLOAD_FIELD = "payload";

  public static final String ATTRIBUTES_KEY_FIELD = "key";
  public static final String ATTRIBUTES_VALUES_FIELD = "values";

  public static final Schema ATTRIBUTES_ENTRY_SCHEMA =
      Schema.builder()
          .addStringField(ATTRIBUTES_KEY_FIELD)
          .addArrayField(ATTRIBUTES_VALUES_FIELD, Schema.FieldType.BYTES)
          .build();
  public static final Schema.FieldType ATTRIBUTES_FIELD_TYPE =
      Schema.FieldType.array(Schema.FieldType.row(ATTRIBUTES_ENTRY_SCHEMA));
  public static final Schema STANDARD_SCHEMA =
      Schema.builder()
          .addByteArrayField(MESSAGE_KEY_FIELD)
          .addDateTimeField(EVENT_TIMESTAMP_FIELD)
          .addArrayField(ATTRIBUTES_FIELD, Schema.FieldType.row(ATTRIBUTES_ENTRY_SCHEMA))
          .addByteArrayField(PAYLOAD_FIELD)
          .build();
}
