package com.google.pubsublite.kafka.sink;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemasTest {

  private static List<Schema> primitiveSchemas() {
    return ImmutableList
        .of(Schema.INT8_SCHEMA, Schema.INT16_SCHEMA, Schema.INT32_SCHEMA, Schema.INT64_SCHEMA,
            Schema.FLOAT32_SCHEMA, Schema.FLOAT64_SCHEMA, Schema.BOOLEAN_SCHEMA,
            Schema.STRING_SCHEMA, Schema.BYTES_SCHEMA);
  }

  private static Object example(Schema.Type t) {
    ImmutableMap.Builder<Schema.Type, Object> values = ImmutableMap.builder();
    values.put(Schema.Type.INT8, (byte) 3);
    values.put(Schema.Type.INT16, (short) 4);
    values.put(Schema.Type.INT32, (int) 5);
    values.put(Schema.Type.INT64, (long) 6L);
    values.put(Schema.Type.FLOAT32, (float) 2.5);
    values.put(Schema.Type.FLOAT64, (double) 3.5);
    values.put(Schema.Type.BOOLEAN, (Boolean) true);
    values.put(Schema.Type.STRING, "abc");
    values.put(Schema.Type.BYTES, ByteString.copyFromUtf8("def").toByteArray());
    return values.build().get(t);
  }

  private static Value exampleValue(Schema.Type t) {
    Value.Builder single = Value.newBuilder();
    switch (t) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
        single.setNumberValue(((Number) example(t)).doubleValue());
        break;
      case BOOLEAN:
        single.setBoolValue((Boolean) example(t));
        break;
      case STRING:
        single.setStringValue(example(t).toString());
        break;
      case BYTES:
        single.setStringValue(Base64.getEncoder().encodeToString((byte[]) example(t)));
        break;
      default:
        throw new RuntimeException("");
    }
    return single.build();
  }

  @Test
  public void testConvertPrimitives() {
    assertThat(Schemas.encodeToBytes(Schema.INT8_SCHEMA, (byte) 3))
        .isEqualTo(ByteString.copyFromUtf8("3"));
    assertThat(Schemas.encodeToBytes(Schema.INT16_SCHEMA, (short) 4))
        .isEqualTo(ByteString.copyFromUtf8("4"));
    assertThat(Schemas.encodeToBytes(Schema.INT32_SCHEMA, 5))
        .isEqualTo(ByteString.copyFromUtf8("5"));
    assertThat(Schemas.encodeToBytes(Schema.INT64_SCHEMA, 6L))
        .isEqualTo(ByteString.copyFromUtf8("6"));
    assertThat(Schemas.encodeToBytes(Schema.FLOAT32_SCHEMA, (float) 2.5))
        .isEqualTo(ByteString.copyFromUtf8(Double.toString(2.5)));
    assertThat(Schemas.encodeToBytes(Schema.FLOAT64_SCHEMA, (float) 3.5))
        .isEqualTo(ByteString.copyFromUtf8(Double.toString(3.5)));
    assertThat(Schemas.encodeToBytes(Schema.BOOLEAN_SCHEMA, true))
        .isEqualTo(ByteString.copyFromUtf8("true"));
    assertThat(Schemas.encodeToBytes(null, "abc")).isEqualTo(ByteString.copyFromUtf8("abc"));
    assertThat(Schemas.encodeToBytes(Schema.STRING_SCHEMA, "def"))
        .isEqualTo(ByteString.copyFromUtf8("def"));
    assertThat(Schemas
        .encodeToBytes(Schema.BYTES_SCHEMA, ByteString.copyFromUtf8("ghi").asReadOnlyByteBuffer()))
        .isEqualTo(ByteString.copyFromUtf8("ghi"));
    assertThat(
        Schemas.encodeToBytes(Schema.BYTES_SCHEMA, ByteString.copyFromUtf8("jkl").toByteArray()))
        .isEqualTo(ByteString.copyFromUtf8("jkl"));
  }

  @Test
  public void testConvertArray() {
    for (Schema schema : primitiveSchemas()) {
      Value expected = Value.newBuilder().setListValue(
          ListValue.newBuilder().addValues(exampleValue(schema.type()))
              .addValues(exampleValue(schema.type()))).build();
      List<Object> objects = ImmutableList.of(example(schema.type()), example(schema.type()));
      assertThat(Schemas.encodeToBytes(SchemaBuilder.array(schema).build(), objects))
          .isEqualTo(expected.toByteString());
    }
  }

  @Test
  public void testConvertStruct() {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct()
        .field("byte", Schema.INT8_SCHEMA)
        .field("short", Schema.INT16_SCHEMA)
        .field("int", Schema.INT32_SCHEMA)
        .field("long", Schema.INT64_SCHEMA)
        .field("float", Schema.FLOAT32_SCHEMA)
        .field("double", Schema.FLOAT64_SCHEMA)
        .field("bool", Schema.BOOLEAN_SCHEMA)
        .field("bytes", Schema.BYTES_SCHEMA)
        .field("string", Schema.STRING_SCHEMA);
    org.apache.kafka.connect.data.Struct kafkaStruct = new org.apache.kafka.connect.data.Struct(
        schemaBuilder.build());
    Struct.Builder struct = Struct.newBuilder();
    kafkaStruct.put("byte", (byte) 3);
    struct.putFields("byte", Value.newBuilder().setNumberValue(3).build());
    kafkaStruct.put("short", (short) 4);
    struct.putFields("short", Value.newBuilder().setNumberValue(4).build());
    kafkaStruct.put("int", (int) 5);
    struct.putFields("int", Value.newBuilder().setNumberValue(5).build());
    kafkaStruct.put("long", (long) 6L);
    struct.putFields("long", Value.newBuilder().setNumberValue(6).build());
    kafkaStruct.put("float", (float) 2.5);
    struct.putFields("float", Value.newBuilder().setNumberValue(2.5).build());
    kafkaStruct.put("double", (double) 3.5);
    struct.putFields("double", Value.newBuilder().setNumberValue(3.5).build());
    kafkaStruct.put("bool", true);
    struct.putFields("bool", Value.newBuilder().setBoolValue(true).build());
    kafkaStruct.put("bytes", ByteString.copyFromUtf8("abc").toByteArray());
    struct.putFields("bytes", Value.newBuilder().setStringValue(
        Base64.getEncoder().encodeToString(ByteString.copyFromUtf8("abc").toByteArray())).build());
    kafkaStruct.put("string", "def");
    struct.putFields("string", Value.newBuilder().setStringValue("def").build());
    Value value = Value.newBuilder().setStructValue(struct).build();

    assertThat(Schemas.encodeToBytes(kafkaStruct.schema(), kafkaStruct))
        .isEqualTo(value.toByteString());
  }

  @Test
  public void testConvertMap() {
    for (Schema keySchema : primitiveSchemas()) {
      for (Schema valueSchema : primitiveSchemas()) {
        Schema mapSchema = SchemaBuilder.map(keySchema, valueSchema).build();
        String key;
        switch (keySchema.type()) {
          case INT8:
          case INT16:
          case INT32:
          case INT64:
            key = Long.toString(((Number) example(keySchema.type())).longValue());
            break;
          case FLOAT32:
          case FLOAT64:
            key = Double.toString(((Number) example(keySchema.type())).doubleValue());
            break;
          case BOOLEAN:
            key = Boolean.toString((Boolean) example(keySchema.type()));
            break;
          case STRING:
            key = example(keySchema.type()).toString();
            break;
          case BYTES:
            key = Base64.getEncoder().encodeToString((byte[]) example(keySchema.type()));
            break;
          default:
            throw new RuntimeException("");
        }
        Map<Object, Object> map = ImmutableMap
            .of(example(keySchema.type()), example(valueSchema.type()));
        Value expected = Value.newBuilder()
            .setStructValue(Struct.newBuilder().putFields(key, exampleValue(valueSchema.type())))
            .build();
        assertThat(Schemas.encodeToBytes(mapSchema, map)).isEqualTo(expected.toByteString());
      }
    }
  }

  @Test
  public void testConvertComplexSchema() {
    Schema schema = SchemaBuilder.struct()
        .field("field",
            SchemaBuilder.array(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA)))
        .build();
    org.apache.kafka.connect.data.Struct kafkaStruct = new org.apache.kafka.connect.data.Struct(
        schema);
    Map<Object, Object> map = ImmutableMap.of("one", true, "two", false);
    List<Object> array = ImmutableList.of(map, map);
    kafkaStruct.put("field", array);
    Value expectedMap = Value.newBuilder().setStructValue(Struct.newBuilder()
        .putFields("one", Value.newBuilder().setBoolValue(true).build())
        .putFields("two", Value.newBuilder().setBoolValue(false).build())).build();
    Value expectedArray = Value.newBuilder().setListValue(ListValue.newBuilder()
        .addValues(expectedMap).addValues(expectedMap)).build();
    Value expected = Value.newBuilder().setStructValue(
        Struct.newBuilder().putFields("field", expectedArray)).build();
    assertThat(Schemas.encodeToBytes(schema, kafkaStruct)).isEqualTo(expected.toByteString());
  }
}
