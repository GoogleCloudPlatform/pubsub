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
import static org.mockito.Mockito.spy;

import com.google.pubsub.kafka.source.CloudPubSubSourceConnector;
import com.google.pubsub.kafka.source.CloudPubSubSourceTask;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

/** Tests for {@link CloudPubSubSourceConnector}. */
public class CloudPubSubSourceConnectorTest {

  private CloudPubSubSourceConnector connector;

  @Before
  public void setup() {
    connector = spy(new CloudPubSubSourceConnector());
  }

  @Test(expected = ConnectException.class)
  public void testStartExceptionCase1() {
    connector.start(new HashMap<>());
  }

  @Test
  public void testSourceConnectorTaskClass() {
    assertEquals(CloudPubSubSourceTask.class, connector.taskClass());
  }
}
