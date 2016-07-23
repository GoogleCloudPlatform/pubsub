// Copyright 2016 Google Inc.
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
package com.google.pubsub.kafka;

import static org.junit.Assert.assertEquals;
import com.google.pubsub.kafka.sink.CloudPubSubSinkConnector;
import com.google.pubsub.kafka.sink.CloudPubSubSinkTask;
import com.google.pubsub.kafka.source.CloudPubSubSourceConnector;
import com.google.pubsub.kafka.source.CloudPubSubSourceTask;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link CloudPubSubSourceConnector} and {@link CloudPubSubSinkConnector}. */
public class CloudPubSubConnectorTest {

  private CloudPubSubSourceConnector sourceConnector;
  private CloudPubSubSinkConnector sinkConnector;

  @Before
  public void setup() {
    sourceConnector = new CloudPubSubSourceConnector();
    sinkConnector = new CloudPubSubSinkConnector();
  }

  @Test
  public void testSourceConnectorTaskClass() {
    assertEquals(CloudPubSubSourceTask.class, sourceConnector.taskClass());
  }

  @Test
  public void testSinkConnectorTaskClass() {
    assertEquals(CloudPubSubSinkTask.class, sinkConnector.taskClass());
  }
}
