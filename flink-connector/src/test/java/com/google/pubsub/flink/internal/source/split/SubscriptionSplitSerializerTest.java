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
package com.google.pubsub.flink.internal.source.split;

import static com.google.common.truth.Truth.assertThat;

import com.google.pubsub.v1.ProjectSubscriptionName;
import org.junit.Test;

public class SubscriptionSplitSerializerTest {

  @Test
  public void testSerialization() throws Exception {
    SubscriptionSplit split =
        SubscriptionSplit.create(ProjectSubscriptionName.of("project", "subscription"));
    SubscriptionSplitSerializer serializer = new SubscriptionSplitSerializer();
    assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(split)))
        .isEqualTo(split);
  }
}
