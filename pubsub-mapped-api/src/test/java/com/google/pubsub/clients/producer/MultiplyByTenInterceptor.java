// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////

package com.google.pubsub.clients.producer;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerInterceptor;

public class MultiplyByTenInterceptor implements ProducerInterceptor<String, Integer> {
  @Override
  public ProducerRecord<String, Integer> onSend(ProducerRecord<String, Integer> producerRecord) {
    int updatedValue = 10 * producerRecord.value();
    System.out.print(updatedValue);
    return new ProducerRecord<String, Integer>(producerRecord.topic(), producerRecord.key(), updatedValue);
  }

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) { }

  @Override
  public void close() {
    System.out.print("Closed");
  }

  @Override
  public void configure(Map<String, ?> map) { }
}