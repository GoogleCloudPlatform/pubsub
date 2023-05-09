/*
 * Copyright 2023 Google LLC
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
package com.google.pubsub.flink.internal.sink;

import com.google.pubsub.flink.PubSubSerializationSchema;
import java.io.IOException;
import org.apache.flink.api.connector.sink2.SinkWriter;

public class PubSubSinkWriter<T> implements SinkWriter<T> {
  private final FlushablePublisher publisher;
  private final PubSubSerializationSchema<T> schema;

  public PubSubSinkWriter(FlushablePublisher publisher, PubSubSerializationSchema<T> schema) {
    this.publisher = publisher;
    this.schema = schema;
  }

  @Override
  public void write(T value, Context context) throws InterruptedException {
    publisher.publish(schema.serialize(value));
  }

  @Override
  public void flush(boolean endOfInput) throws IOException {
    publisher.flush();
  }

  @Override
  public void close() throws IOException {
    publisher.flush();
  }
}
