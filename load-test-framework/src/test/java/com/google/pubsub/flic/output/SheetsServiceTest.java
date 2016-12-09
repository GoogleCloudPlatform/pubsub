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

package com.google.pubsub.flic.output;

import com.google.pubsub.flic.controllers.Client;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import com.google.pubsub.flic.controllers.Client.ClientType;
import com.google.pubsub.flic.controllers.ClientParams;
import com.google.pubsub.flic.controllers.Controller;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Tests for {@link SheetsService}.
 */
public class SheetsServiceTest {

  @Test
  public void testClientSwitch() {
    Map<String, Map<ClientParams, Integer>> types = new HashMap<>();
    Map<ClientParams, Integer> paramsMap = new HashMap<>();
    for (ClientType type : ClientType.values()) {
      paramsMap.put(new ClientParams(type, ""), 1);
    }
    types.put("zone-test", paramsMap);
    SheetsService service = new SheetsService(null, types);

    Map<Client.ClientType, Integer> countMap = service.getCountMap();
    for (ClientType t : ClientType.values()) {
      assertEquals(countMap.get(t).intValue(), 1);
    }

    Map<ClientType, Controller.LoadtestStats> stats = new HashMap<>();
    for (ClientType type : ClientType.values()) {
      stats.put(type, null);
      try {
        service.getValuesList(stats);
      } catch (Exception e) {
        assertTrue(e instanceof NullPointerException);
      }
      // Remove type so only the next type in the enum will be tested.
      stats.remove(type);
    }
  }

  @Test
  public void testZeroClientCounts() {
    Map<String, Map<ClientParams, Integer>> types = new HashMap<>();
    Map<ClientParams, Integer> paramsMap = new HashMap<>();
    for (ClientType type : ClientType.values()) {
      paramsMap.put(new ClientParams(type, ""), 0);
    }
    types.put("zone-test", paramsMap);
    SheetsService service = new SheetsService(null, types);

    Map<Client.ClientType, Integer> countMap = service.getCountMap();
    for (ClientType t : ClientType.values()) {
      assertEquals(countMap.get(t).intValue(), 0);
    }
  }
}
