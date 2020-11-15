package com.google.pubsublite.kafka.sink;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

/**
 * Schema handling for Pub/Sub Lite.
 *
 * null schemas are treated as Schema.STRING_SCHEMA
 *
 * Top level BYTES payloads are unmodified.
 * Top level STRING payloads are encoded using copyFromUtf8.
 * Top level Integral payloads are converted using copyFromUtf8(Long.toString(x.longValue()))
 * Top level Floating point payloads are converted using
 * copyFromUtf8(Double.toString(x.doubleValue()))
 *
 * All other payloads are encoded into a protobuf Value, then converted to a ByteString.
 * Nested STRING fields are encoded into a protobuf Value.
 * Nested BYTES fields are encoded to a protobuf Value holding the base64 encoded bytes.
 * Nested Numeric fields are encoded as a double into a protobuf Value.
 *
 * Maps with Array, Map, or Struct keys are not supported.
 * BYTES keys in maps are base64 encoded.
 * Integral keys are converted using Long.toString(x.longValue())
 * Floating point keys are converted using Double.toString(x.doubleValue())
 */
final class Schemas {

  private Schemas() {
  }

  private static Schema.Type safeSchemaType(@Nullable Schema schema) {
    if (schema == null) {
      return Schema.Type.STRING;
    }
    return schema.type();
  }

  static ByteString encodeToBytes(@Nullable Schema schema, Object object) {
    switch (safeSchemaType(schema)) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
      case BOOLEAN:
      case STRING:
        return ByteString.copyFromUtf8(stringRep(schema, object));
      case BYTES:
        return extractBytes(object);
      case ARRAY:
      case MAP:
      case STRUCT:
        return encode(schema, object).toByteString();
    }
    throw new DataException("Invalid schema type.");
  }

  @VisibleForTesting
  static Value encode(@Nullable Schema schema, Object object) {
    switch (safeSchemaType(schema)) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
        return toValue((Number) object);
      case BOOLEAN:
        return Value.newBuilder().setBoolValue((Boolean) object).build();
      case STRING:
        return Value.newBuilder().setStringValue(object.toString()).build();
      case BYTES:
        ByteString bytes = extractBytes(object);
        return Value.newBuilder()
            .setStringValue(Base64.getEncoder().encodeToString(bytes.toByteArray())).build();
      case ARRAY: {
        ListValue.Builder listBuilder = ListValue.newBuilder();
        List<Object> objects = (List<Object>) object;
        for (Object o : objects) {
          listBuilder.addValues(encode(schema.valueSchema(), o));
        }
        return Value.newBuilder().setListValue(listBuilder).build();
      }
      case MAP: {
        Struct.Builder builder = Struct.newBuilder();
        Map<Object, Object> map = (Map<Object, Object>) object;
        for (Object key : map.keySet()) {
          builder.putFields(stringRep(schema.keySchema(), key),
              encode(schema.valueSchema(), map.get(key)));
        }
        return Value.newBuilder().setStructValue(builder).build();
      }
      case STRUCT: {
        Struct.Builder builder = Struct.newBuilder();
        org.apache.kafka.connect.data.Struct struct = (org.apache.kafka.connect.data.Struct) object;
        for (Field f : schema.fields()) {
          builder.putFields(f.name(), encode(f.schema(), struct.get(f)));
        }
        return Value.newBuilder().setStructValue(builder).build();
      }
    }
    throw new DataException("Invalid schema type.");
  }

  private static String stringRep(@Nullable Schema schema, Object object) {
    switch (safeSchemaType(schema)) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
        return Long.toString(((Number) object).longValue());
      case FLOAT32:
      case FLOAT64:
        return Double.toString(((Number) object).doubleValue());
      case BOOLEAN:
      case STRING:
        return object.toString();
      case BYTES:
        return Base64.getEncoder().encodeToString(extractBytes(object).toByteArray());
      case ARRAY:
      case MAP:
      case STRUCT:
        throw new DataException("Cannot convert ARRAY, MAP, or STRUCT to String.");
    }
    throw new DataException("Invalid schema type.");
  }

  private static ByteString extractBytes(Object object) {
    if (object instanceof byte[]) {
      return ByteString.copyFrom((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      return ByteString.copyFrom((ByteBuffer) object);
    }
    throw new DataException("Unexpected value class with BYTES schema type.");
  }

  private static Value toValue(Number val) {
    return Value.newBuilder().setNumberValue(val.doubleValue()).build();
  }
}
