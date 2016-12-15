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
package com.google.pubsub.kafka.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.protobuf.ByteString;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link ByteStringConverter}. */
public class ByteStringConverterTest {

  private static final String TOPIC = "test";
  private static final String DATA = "This is a test";
  private static final String SCHEMA_NAME = "dummy name";

  private ByteStringConverter converter;

  @Before
  public void setup() {
    converter = new ByteStringConverter();
    converter.configure(Collections.emptyMap(), false);
  }

  @Test
  public void testToConnectData() {
    byte[] value = DATA.getBytes();
    SchemaAndValue expected =
        new SchemaAndValue(
            SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build(),
            ByteString.copyFrom(value));
    assertEquals(expected, converter.toConnectData(TOPIC, value));
    assertEquals(SchemaAndValue.NULL, converter.toConnectData(TOPIC, null));
  }

  @Test
  public void testFromConnectData() {
    String expected = DATA;
    Schema schema = SchemaBuilder.bytes().name(ConnectorUtils.SCHEMA_NAME).build();
    byte[] result = converter.fromConnectData(TOPIC, schema, ByteString.copyFromUtf8(expected));
    assertEquals(expected, new String(result));
    assertNull(converter.fromConnectData(TOPIC, schema, null));
  }

  @Test(expected = DataException.class)
  public void testFromConnectDataExceptionCase() {
    Schema schema = SchemaBuilder.bytes().name(SCHEMA_NAME).build();
    converter.fromConnectData(TOPIC, schema, null);
  }
}
