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
package com.google.pubsub.flink;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.Serializable;
import org.apache.flink.api.common.serialization.SerializationSchema;

public interface PubSubSerializationSchema<T> extends Serializable {
  static <T> PubSubSerializationSchema<T> dataOnly(SerializationSchema<T> schema) {
    return new PubSubSerializationSchema<T>() {

      @Override
      public void open(SerializationSchema.InitializationContext context) throws Exception {
        schema.open(context);
      }

      @Override
      public PubsubMessage serialize(T value) {
        return PubsubMessage.newBuilder()
            .setData(ByteString.copyFrom(schema.serialize(value)))
            .build();
      }
    };
  }

  void open(SerializationSchema.InitializationContext context) throws Exception;

  PubsubMessage serialize(T value);
}
