package com.google.pubsub.jms.light.integration;

import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.Subscription;
import com.google.cloud.pubsub.SubscriptionInfo;
import com.google.cloud.pubsub.TopicInfo;
import com.google.cloud.pubsub.testing.LocalPubSubHelper;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class BaseIntegrationTest
{
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseIntegrationTest.class);

  public static String PROJECT_ID = "jms-light";
  public static String TOPIC_NAME = "test_topic";
  public static String SUBSCRIPTION_NAME = "test_topic_first";

  private static int EMBEDDED_SERVICE_PORT;
  private static LocalPubSubHelper EMBEDDED_PUBSUB_SERVICE;
  private static Subscription SUBSCRIPTION;

  @BeforeClass
  public static void startPubSub() throws IOException, InterruptedException
  {
    EMBEDDED_PUBSUB_SERVICE = LocalPubSubHelper.create();
    EMBEDDED_SERVICE_PORT = EMBEDDED_PUBSUB_SERVICE.getPort();

    final PubSub pubSubService = EMBEDDED_PUBSUB_SERVICE.getOptions()
        .toBuilder()
        .setProjectId(PROJECT_ID)
        .setHost("localhost:" + EMBEDDED_SERVICE_PORT)
        .build()
        .getService();

    EMBEDDED_PUBSUB_SERVICE.start();

    pubSubService.create(TopicInfo.of(TOPIC_NAME));
    SUBSCRIPTION = pubSubService.create(SubscriptionInfo.of(TOPIC_NAME, SUBSCRIPTION_NAME));

    LOGGER.info("       topics: {}", pubSubService.listTopics().getValues());
    LOGGER.info("subscriptions: {}", pubSubService.listSubscriptions().getValues());
  }

  @AfterClass
  public static void shutdownPubSub() throws InterruptedException, TimeoutException, IOException
  {
    EMBEDDED_PUBSUB_SERVICE.stop(Duration.standardSeconds(10L));
  }

  public String getServiceHost()
  {
    return "localhost";
  }

  public int getServicePort()
  {
    return EMBEDDED_SERVICE_PORT;
  }

  public Subscription getServiceSubscription()
  {
    return SUBSCRIPTION;
  }
}
