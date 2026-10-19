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

import java.io.Serializable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

public class CpsDataWriterFactory implements Serializable, DataWriterFactory<InternalRow> {

  private static final long serialVersionUID = -6904546364310978844L;

  private static final CachedPublishers CACHED_PUBLISHERS = new CachedPublishers();

  private final StructType inputSchema;
  private final WriteDataSourceOptions writeOptions;

  public CpsDataWriterFactory(StructType inputSchema, WriteDataSourceOptions writeOptions) {
    this.inputSchema = inputSchema;
    this.writeOptions = writeOptions;
  }

  @Override
  public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
    return new CpsDataWriter(
        partitionId, taskId, epochId, inputSchema, CACHED_PUBLISHERS.getOrCreate(writeOptions));
  }
}
