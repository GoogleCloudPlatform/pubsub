package com.google.pubsub.kafka;

import com.google.protobuf.ByteString;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

/**
 * ByteStringConverter publishes records to a Google Cloud Pub/Sub topic.
 */
public class ByteStringConverter implements Converter {
  private static final String SCHEMA_NAME = ByteString.class.getName();

  public ByteStringConverter() {}

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    if (!schema.name().equals(SCHEMA_NAME)) {
      throw new DataException("Object of type " + schema.name() +
                              "cannot be converted by ByteStringConverter.");
    }
    return value == null ? null : ((ByteString)value).toByteArray();
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    if (value == null) {
      return SchemaAndValue.NULL;
    }
    try {
      return new SchemaAndValue(SchemaBuilder.bytes().name(SCHEMA_NAME).build(),
                                ByteString.copyFrom(value));
    } catch (Exception e) {
      throw new DataException("Could not convert value: ", e);
    }
  }
}
