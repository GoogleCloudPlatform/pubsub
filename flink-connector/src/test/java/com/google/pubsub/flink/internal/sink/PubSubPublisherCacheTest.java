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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.verify;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubPublisherCacheTest {
  @Mock Publisher publisher1;
  @Mock Publisher publisher2;

  @Test
  public void getOrCreate_cachesPublishers() throws Exception {
    TopicName topic1 = TopicName.of("project1", "topic1");
    TopicName topic2 = TopicName.of("project2", "topic2");

    assertThat(PubSubPublisherCache.getOrCreate(topic1, (topic) -> publisher1))
        .isEqualTo(publisher1);
    assertThat(PubSubPublisherCache.getOrCreate(topic1, (topic) -> publisher2))
        .isEqualTo(publisher1);
    assertThat(PubSubPublisherCache.getOrCreate(topic2, (topic) -> publisher2))
        .isEqualTo(publisher2);
  }

  @Test
  public void close_shutsdownPublishers() throws Exception {
    assertThat(
            PubSubPublisherCache.getOrCreate(
                TopicName.of("project1", "topic1"), (topic) -> publisher1))
        .isEqualTo(publisher1);
    assertThat(
            PubSubPublisherCache.getOrCreate(
                TopicName.of("project2", "topic2"), (topic) -> publisher2))
        .isEqualTo(publisher2);

    PubSubPublisherCache.close();
    verify(publisher1).shutdown();
    verify(publisher2).shutdown();
  }
}
