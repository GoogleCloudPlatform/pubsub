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

import com.google.auto.service.AutoService;
import java.util.Optional;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.StreamWriteSupport;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;

@AutoService(DataSourceRegister.class)
public final class DataSource
    implements DataSourceV2,
    ContinuousReadSupport,
    StreamWriteSupport,
    DataSourceRegister {

  @Override
  public String shortName() {
    return "pubsub";
  }

  @Override
  public ContinuousReader createContinuousReader(
      Optional<StructType> schema, String checkpointLocation, DataSourceOptions options) {
    if (schema.isPresent()) {
      throw new IllegalArgumentException(
          "PubSub Lite uses fixed schema and custom schema is not allowed");
    }

    ReadDataSourceOptions dataSourceOptions =
        ReadDataSourceOptions.fromSparkDataSourceOptions(options);
    return new CpsContinuousReader(
        dataSourceOptions::getPullSubscriber, dataSourceOptions.getCommitter(), dataSourceOptions.subscription(), dataSourceOptions.readShards()
    );
  }

  @Override
  public StreamWriter createStreamWriter(
      String queryId, StructType schema, OutputMode mode, DataSourceOptions options) {
    SparkUtils.verifyWriteInputSchema(schema);
    return new CpsStreamWriter(schema, WriteDataSourceOptions.fromSparkDataSourceOptions(options));
  }
}
