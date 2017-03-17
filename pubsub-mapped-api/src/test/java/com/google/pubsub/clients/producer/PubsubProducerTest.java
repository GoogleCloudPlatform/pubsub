/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.pubsub.clients.producer;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.powermock.api.easymock.PowerMock.createMock;
import com.google.pubsub.clients.producer.PubsubProducer.Builder;
import com.google.pubsub.common.PubsubChannelUtil;
import com.google.pubsub.v1.PublisherGrpc.PublisherFutureStub;
import java.io.IOException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

//@RunWith(PowerMockRunner.class)
//@PrepareForTest(PublisherFutureStub.class)
public class PubsubProducerTest {

  private static final String TOPIC = "testTopic";
  private static final String MESSAGE = "testMessage";
  private static final String PROJECT = "unit-test-proj";
  private static final StringSerializer testSerializer = new StringSerializer();
  private static final ProducerRecord testRecord = new ProducerRecord(TOPIC, MESSAGE);

   private static PublisherFutureStub mockStub;
  @Mock private static PubsubChannelUtil mockChannelUtil;

  private boolean channelIsClosed;


  @Before
  public void setUp() {
  //  mockStub = createMock(PublisherFutureStub.class);
    MockitoAnnotations.initMocks(this);
    channelIsClosed = false;

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        return channelIsClosed = true;
      }
    }).when(mockChannelUtil).closeChannel();
  }

  /* send() tests */
  @Test
  public void testSendPublisherClosed() throws IOException {
   /* PubsubProducer testProducer = getNewProducer();
    testProducer.close();

    try {
      testProducer.send(testRecord);
      fail("Should have thrown a runtime exception.");
    } catch (RuntimeException expected) {
      assertTrue("Channel should be closed.", channelIsClosed);
    }*/
  }

  @Test
  public void testSendRecordTooLarge() {

  }

  @Test
  public void testSendBatchFull() {

  }

  /* This happens when the batch size is > 1 and not enough messages are batched */
  @Test
  public void testSendMessageNotSentYet() {

  }

  /* flush() tests */
  @Test
  public void testFlushMessagesSent() {

  }

  /* close() tests */
  @Test
  public void testCloseTimeoutLessThanZero() {

  }

  @Test
  public void testCloseChannelCloseSuccessful() {

  }

}
