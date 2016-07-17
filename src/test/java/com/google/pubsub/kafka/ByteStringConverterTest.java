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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.protobuf.ByteString;
import com.google.pubsub.kafka.common.ByteStringConverter;
import com.google.pubsub.kafka.common.ConnectorUtils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;

import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

/**
 * Tests for {@link ByteStringConverter}.
 */
public class ByteStringConverterTest {

  String topic = "test";
  ByteStringConverter converter = new ByteStringConverter();

  @Before
  public void setup() {
    converter.configure(Collections.emptyMap(), false);
  }

  @Test
  public void testToConnectData() {
    byte[] value = "This is a test".getBytes();
    SchemaAndValue expected = new SchemaAndValue(
        SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build(), ByteString.copyFrom(value));
    assertEquals(expected, converter.toConnectData(topic, value));
    // Check case when byte array parameter is null.
    assertEquals(SchemaAndValue.NULL, converter.toConnectData(topic, null));
  }

  public void testToConnectDataExceptionCase() {
   // TODO(rramkumar): Implement.
  }

  @Test
  public void testFromConnectData() {
    String expected = "this is a test";
    Schema schema = SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build();
    byte[] result = converter.fromConnectData(topic, schema, ByteString.copyFromUtf8(expected));
    assertEquals(expected, new String(result));
    assertNull(converter.fromConnectData(topic, schema, null));
  }

  @Test(expected=DataException.class)
  public void testFromConnectDataExceptionCase() {
    Schema schema = SchemaBuilder.bytes().name("dummy name").build();
    converter.fromConnectData(topic, schema, null);
  }
}
