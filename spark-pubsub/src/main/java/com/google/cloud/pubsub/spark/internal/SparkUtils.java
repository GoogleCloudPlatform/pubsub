/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pubsub.spark.internal;

import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.v1.SubscriptionName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.ByteArray;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Option;
import scala.compat.java8.functionConverterImpls.FromJavaBiConsumer;

import static scala.collection.JavaConverters.asScalaBufferConverter;

public class SparkUtils {

  private static final GoogleLogger log = GoogleLogger.forEnclosingClass();

  public static ArrayBasedMapData convertAttributesToSparkMap(Map<String, String> attributeMap) {
    List<UTF8String> keyList = new ArrayList<>();
    List<UTF8String> valueList = new ArrayList<>();

    attributeMap.forEach(
        (key, value) -> {
          keyList.add(UTF8String.fromString(key));
          valueList.add(UTF8String.fromString(value));
        });

    return new ArrayBasedMapData(
        new GenericArrayData(asScalaBufferConverter(keyList).asScala()),
        new GenericArrayData(asScalaBufferConverter(valueList).asScala()));
  }

  public static InternalRow toInternalRow(PubsubMessage message, SubscriptionName subscription) {
    List<Object> list = ImmutableList.of(
        subscription.toString(),
        message.getOrderingKey(),
        ByteArray.concat(message.getData().toByteArray()),
        Timestamps.toMicros(message.getPublishTime()),
        convertAttributesToSparkMap(message.getAttributesMap())
    );
    return InternalRow.apply(asScalaBufferConverter(list).asScala());
  }

  @SuppressWarnings("unchecked")
  private static <T> void extractVal(
      StructType inputSchema,
      InternalRow row,
      String fieldName,
      DataType expectedDataType,
      Consumer<T> consumer) {
    Option<Object> idxOr = inputSchema.getFieldIndex(fieldName);
    if (!idxOr.isEmpty()) {
      Integer idx = (Integer) idxOr.get();
      // DateType should match and not throw ClassCastException, as we already verified
      // type match in driver node.
      consumer.accept((T) row.get(idx, expectedDataType));
    }
  }

  @SuppressWarnings("CheckReturnValue")
  public static PubsubMessage toPubSubMessage(StructType inputSchema, InternalRow row) {
    PubsubMessage.Builder builder = PubsubMessage.newBuilder();
    extractVal(
        inputSchema,
        row,
        "ordering_key",
        SparkStructs.PUBLISH_FIELD_TYPES.get("ordering_key"),
        builder::setOrderingKey);
    extractVal(
        inputSchema,
        row,
        "data",
        SparkStructs.PUBLISH_FIELD_TYPES.get("data"),
        (byte[] o) -> builder.setData(ByteString.copyFrom(o)));
    extractVal(
        inputSchema,
        row,
        "attributes",
        SparkStructs.PUBLISH_FIELD_TYPES.get("attributes"),
        (MapData o) -> {
          o.foreach(
              DataTypes.StringType,
              DataTypes.StringType,
              new FromJavaBiConsumer<>(
                  (Object k, Object v) -> {
                    builder.putAttributes(k.toString(), v.toString());
                  }));
        });
    return builder.build();
  }

  /**
   * Make sure data fields for publish have expected Spark DataType if they exist.
   *
   * @param inputSchema input table schema to write to Pub/Sub Lite.
   * @throws IllegalArgumentException if any DataType mismatch detected.
   */
  public static void verifyWriteInputSchema(StructType inputSchema) {
    SparkStructs.PUBLISH_FIELD_TYPES.forEach(
        (k, v) -> {
          Option<Object> idxOr = inputSchema.getFieldIndex(k);
          if (!idxOr.isEmpty()) {
            StructField f = inputSchema.apply((int) idxOr.get());
            if (!f.dataType().sameType(v)) {
              throw new IllegalArgumentException(
                  String.format(
                      "Column %s in input schema to write to "
                          + "Pub/Sub Lite has a wrong DataType. Actual: %s, expected: %s.",
                      k, f.dataType(), v));
            }
          } else {
            log.atInfo().atMostEvery(5, TimeUnit.MINUTES).log(
                "Input schema to write "
                    + "to Pub/Sub Lite doesn't contain %s column, this field for all rows will "
                    + "be set to empty.",
                k);
          }
        });
  }
}
