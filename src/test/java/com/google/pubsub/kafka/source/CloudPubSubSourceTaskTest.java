package com.google.pubsub.kafka.source;

import com.google.pubsub.kafka.sink.CloudPubSubSinkTask;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/**
 * Created by rramkumar on 7/16/16.
 */
public class CloudPubSubSourceTaskTest  {

  private CloudPubSubSourceTask sourceTask;

  @Before
  public void setup() {
    sourceTask = new CloudPubSubSourceTask();
  }

  @Test
  public void testPoll() {
    sourceTask.subscriber = mock(CloudPubSubSubscriber.class);
    doReturn(null).when(sourceTask.subscriber).ackMessages()
  }
}
