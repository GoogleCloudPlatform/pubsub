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

package com.google.pubsub.clients.consumer;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class PubsubConsumer<K, V> implements Consumer<K, V> {

  public PubsubConsumer(Map<String, Object> configs) {

  }

  public PubsubConsumer(Map<String, Object> configs,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {

  }

  public PubsubConsumer(Properties properties) {

  }

  public PubsubConsumer(Properties properties,
      Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {

  }

  public Set<TopicPartition> assignment() {
    throw new NotImplementedException("Not yet implemented");
  }

  public Set<String> subscription() {
    throw new NotImplementedException("Not yet implemented");
  }

  public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void subscribe(Collection<String> topics) {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
    throw new NotImplementedException("Not yet implemented");
  }

  public void unsubscribe() {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public ConsumerRecords<K, V> poll(long timeout) {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void commitSync() {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void commitAsync() {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets,
      OffsetCommitCallback callback) {
    throw new NotImplementedException("Not yet implemented");
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    throw new NotImplementedException("Not yet implemented");
  }

  public void seekToBeginning(Collection<TopicPartition> partitions) {

  }

  public void seekToEnd(Collection<TopicPartition> partitions) {
    throw new NotImplementedException("Not yet implemented");
  }

  public long position(TopicPartition partition) {
    throw new NotImplementedException("Not yet implemented");
  }

  public OffsetAndMetadata committed(TopicPartition partition) {
    throw new NotImplementedException("Not yet implemented");
  }

  public Map<MetricName, ? extends Metric> metrics() {
    throw new NotImplementedException("Not yet implemented");
  }

  public List<PartitionInfo> partitionsFor(String topic) {
    throw new NotImplementedException("Not yet implemented");
  }

  public Map<String, List<PartitionInfo>> listTopics() {
    throw new NotImplementedException("Not yet implemented");
  }

  public void pause(Collection<TopicPartition> partitions) {
    throw new NotImplementedException("Not yet implemented");
  }

  public void resume(Collection<TopicPartition> partitions) {
    throw new NotImplementedException("Not yet implemented");
  }

  public Set<TopicPartition> paused() {
    throw new NotImplementedException("Not yet implemented");
  }

  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
    throw new NotImplementedException("Not yet implemented");
  }

  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    throw new NotImplementedException("Not yet implemented");
  }

  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    throw new NotImplementedException("Not yet implemented");
  }

  public void close() {

  }

  public void close(long timeout, TimeUnit timeUnit) {
    throw new NotImplementedException("Not yet implemented");
  }

  public void wakeup() {
    throw new NotImplementedException("Not yet implemented");
  }
}