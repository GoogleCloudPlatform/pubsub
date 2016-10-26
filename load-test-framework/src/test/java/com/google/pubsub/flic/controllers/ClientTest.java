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
package com.google.pubsub.flic.controllers;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.pubsub.flic.common.LatencyDistribution;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Test;

/**
 * Tests for {@link Client}.
 */
public class ClientTest {
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

  @Test
  public void testEmpty() {
    Client client = new Client(Client.ClientType.CPS_GCLOUD_PUBLISHER, "127.0.0.1",
        "my-project", null, executor);
    assertArrayEquals(client.getBucketValues(),
        new long[LatencyDistribution.LATENCY_BUCKETS.length]);
    assertEquals(client.getClientType(), Client.ClientType.CPS_GCLOUD_PUBLISHER);
    assertEquals(client.getRunningSeconds(), 0);
  }

  @Test
  public void testTypes() {
    for (Client.ClientType type : Client.ClientType.values()) {
      // All subscriber types should end with subscriber
      assertTrue(type.getSubscriberType().toString().endsWith("subscriber"));
      // Any type that begins with cps and ends with publisher should be a CPS publisher
      assertEquals(type.toString().startsWith("cps") && type.toString().endsWith("publisher"),
          type.isCpsPublisher());
    }
  }
}
