// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////
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
