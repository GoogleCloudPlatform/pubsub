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
package com.google.cloud.pubsub.republisher.kafka

import com.google.common.collect.ImmutableListMultimap
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import org.apache.kafka.common.record.Record

object TranslationUtils {
  def translateRecord(record: Record): PubsubMessage = {
    val builder = PubsubMessage.newBuilder()
    if (record.value != null && record.valueSize > 0) {
      builder.setData(ByteString.copyFrom(record.value))
    } else {
      // Cloud Pub/Sub does not allow publishing empty payloads.
      builder.setData(ByteString.copyFromUtf8("<empty>"))
    }
    if (record.hasKey) {
      builder.setOrderingKey(ByteString.copyFrom(record.key).toStringUtf8)
    }

    val mapBuilder = ImmutableListMultimap.builder[String, String]
    record.headers().foreach(header => {
      mapBuilder.put(header.key, ByteString.copyFrom(header.value).toStringUtf8)
    })
    // Format attributes as comma separated http headers
    mapBuilder.build.asMap.forEach((key, values) => builder.putAttributes(key, String.join(", ", values)))
    builder.build()
  }

  def translateTopic(topic: String): String = {
    topic.split("\\.").mkString("/")
  }
}
