/* Copyright 2017 Google Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License. */

package com.google.pubsub.clients.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class TestConsumerInterceptor implements ConsumerInterceptor<Integer, String> {

  @Override
  public ConsumerRecords<Integer, String> onConsume(ConsumerRecords<Integer, String> consumerRecords) {
    Map<TopicPartition, List<ConsumerRecord<Integer, String>>> pollRecords = new HashMap<>();

    for(TopicPartition topicPartition: consumerRecords.partitions()) {
      Iterable<ConsumerRecord<Integer, String>> records = consumerRecords.records(topicPartition.topic());
      List<ConsumerRecord<Integer, String>> consumedRecords = new ArrayList<>();
      for(ConsumerRecord next: records) {

        ConsumerRecord newRecord = new ConsumerRecord<>(next.topic(), next.partition(),
            next.offset(), next.timestamp(), next.timestampType(),
            next.checksum(), next.serializedKeySize(), next.serializedValueSize(), -1,
            "_consumed_");

        consumedRecords.add(newRecord);
      }
      pollRecords.put(topicPartition, consumedRecords);
    }

    return new ConsumerRecords<>(pollRecords);
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {

  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}
