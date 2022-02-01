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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkStructs {

  public static MapType ATTRIBUTES_DATATYPE =
      DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);
  public static Map<String, DataType> PUBLISH_FIELD_TYPES =
      ImmutableMap.of(
          "ordering_key", DataTypes.StringType,
          "data", DataTypes.BinaryType,
          "attributes", ATTRIBUTES_DATATYPE);
  public static StructType DEFAULT_SCHEMA =
      new StructType(
          new StructField[]{
              new StructField("subscription", DataTypes.StringType, false, Metadata.empty()),
              new StructField("ordering_key", PUBLISH_FIELD_TYPES.get("ordering_key"), false,
                  Metadata.empty()),
              new StructField("data", PUBLISH_FIELD_TYPES.get("data"), false, Metadata.empty()),
              new StructField("publish_timestamp", DataTypes.TimestampType, false,
                  Metadata.empty()),
              new StructField(
                  "attributes", PUBLISH_FIELD_TYPES.get("attributes"), true, Metadata.empty())
          });
}

