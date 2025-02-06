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
package com.google.pubsub.flink.internal.source.enumerator;

import static com.google.common.truth.Truth.assertThat;

import com.google.pubsub.flink.proto.PubSubEnumeratorCheckpoint;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class PubSubCheckpointSerializerTest {

  @Test
  public void testSerialization() throws Exception {
    List<PubSubEnumeratorCheckpoint.Assignment> assignments = new ArrayList<>();
    assignments.add(PubSubEnumeratorCheckpoint.Assignment.newBuilder().setSubtask(1).build());
    PubSubEnumeratorCheckpoint proto =
        PubSubEnumeratorCheckpoint.newBuilder().addAllAssignments(assignments).build();
    PubSubCheckpointSerializer serializer = new PubSubCheckpointSerializer();
    assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(proto)))
        .isEqualTo(proto);
  }
}
