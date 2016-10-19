package com.google.pubsub.flic.output;

import com.google.pubsub.flic.controllers.Client.ClientType;
import com.google.pubsub.flic.controllers.ClientParams;
import com.google.pubsub.flic.controllers.Controller;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
 * Tests for {@link SheetsService}.
 */
public class SheetsServiceTest {
  
  @Test
  public void testClientSwitch() {
    Map<String, Map<ClientParams, Integer>> types = 
        new HashMap<String, Map<ClientParams, Integer>>();
    int expectedCpsCount = 0;
    int expectedKafkaCount = 0;
    Map<ClientParams, Integer> paramsMap = new HashMap<ClientParams, Integer>();    
    for (ClientType type : ClientType.values()) {
      paramsMap.put(new ClientParams(type, ""), 1);
      if (type.toString().startsWith("cps")) {
        expectedCpsCount++;
      } else if (type.toString().startsWith("kafka")) {
        expectedKafkaCount++;
      } else {
        fail("ClientType toString didn't start with cps or kafka");
      }
    }
    types.put("zone-test", paramsMap);
    SheetsService service = new SheetsService("", types);
    
    assertEquals(
        service.getCpsPublisherCount() + service.getCpsSubscriberCount(), expectedCpsCount);
    assertEquals(
        service.getKafkaPublisherCount() + service.getKafkaSubscriberCount(), expectedKafkaCount);
    
    Map<ClientType, Controller.LoadtestStats> stats = 
        new HashMap<ClientType, Controller.LoadtestStats>();
    for (ClientType type : ClientType.values()) {
      stats.put(type, null);
      try {
        service.getValuesList(stats);
      } catch(Exception e) {
        assertTrue(e instanceof NullPointerException);
      }
      // Remove type so only the next type in the enum will be tested.
      stats.remove(type);
    }
  }
}

