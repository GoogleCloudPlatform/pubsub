package com.google.pubsub.flic.output;

import com.google.pubsub.flic.controllers.Client.ClientType;
import com.google.pubsub.flic.controllers.ClientParams;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/*
 * Tests for {@link SheetsService}.
 */
public class SheetsServiceTest {
  
  public void testClientCount() {
    Map<String, Map<ClientParams, Integer>> types = 
        new HashMap<String, Map<ClientParams, Integer>>();
    Map<ClientParams, Integer> paramsMap = new HashMap<ClientParams, Integer>();
    paramsMap.put(new ClientParams(ClientType.CPS_GCLOUD_PUBLISHER, "sub"), 1);
    paramsMap.put(new ClientParams(ClientType.CPS_GCLOUD_SUBSCRIBER, "sub"), 1);
    paramsMap.put(new ClientParams(ClientType.KAFKA_PUBLISHER, "sub"), 1);
    paramsMap.put(new ClientParams(ClientType.KAFKA_SUBSCRIBER, "sub"), 1);
    types.put("zone", paramsMap);
    
    SheetsService service = new SheetsService("", types);
    
    assertEquals(service.getCpsPublisherCount(), 1);
    assertEquals(service.getCpsSubscriberCount(), 1);
    assertEquals(service.getKafkaPublisherCount(), 1);
    assertEquals(service.getKafkaSubscriberCount(), 1);
  }
}

