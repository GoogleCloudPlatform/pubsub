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
package com.google.pubsub.flink;
import static org.junit.Assert.assertThrows;

import java.lang.NullPointerException;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubSinkTest {
    @Test
    public void build_invalidTopic() throws Exception {
        assertThrows(NullPointerException.class, () -> PubSubSink.<String>builder().withTopicName(null));
        PubSubSink.Builder<String> builder = PubSubSink.<String>builder()
                .withProjectName("project")
                .withSerializationSchema(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()));
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void build_invalidProject() throws Exception {
        assertThrows(NullPointerException.class, () -> PubSubSink.<String>builder().withProjectName(null));
        PubSubSink.Builder<String> builder = PubSubSink.<String>builder()
                .withTopicName("topic")
                .withSerializationSchema(PubSubSerializationSchema.dataOnly(new SimpleStringSchema()));
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    public void build_invalidSchema() throws Exception {
        assertThrows(NullPointerException.class, () -> PubSubSink.<String>builder().withSerializationSchema(null));
        PubSubSink.Builder<String> builder = PubSubSink.<String>builder()
                .withProjectName("project")
                .withTopicName("topic");
        assertThrows(NullPointerException.class, builder::build);
    }
}