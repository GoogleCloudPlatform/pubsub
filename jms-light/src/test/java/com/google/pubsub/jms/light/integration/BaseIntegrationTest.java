package com.google.pubsub.jms.light.integration;

import com.google.cloud.pubsub.deprecated.PubSub;
import com.google.cloud.pubsub.deprecated.Subscription;
import com.google.cloud.pubsub.deprecated.SubscriptionInfo;
import com.google.cloud.pubsub.deprecated.TopicInfo;
import com.google.cloud.pubsub.deprecated.testing.LocalPubSubHelper;
import com.google.common.base.Joiner;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class BaseIntegrationTest {
  private static final Logger LOGGER = Logger.getLogger(BaseIntegrationTest.class.getName());

  public static String PROJECT_ID = "jms-light";
  public static String TOPIC_NAME = "test_topic";
  public static String SUBSCRIPTION_NAME = "test_topic_first";

  private static int EMBEDDED_SERVICE_PORT;
  private static String EMBEDDED_SERVICE_HOST = "127.0.0.1";
  private static LocalPubSubHelper EMBEDDED_PUBSUB_SERVICE;
  private static Subscription SUBSCRIPTION;

  /**
   * This method gets called before every test run and sets up a local pub/sub emulator.
   */
  @BeforeClass
  public static void startPubSub() throws IOException, InterruptedException {
    EMBEDDED_PUBSUB_SERVICE = LocalPubSubHelper.create();
    EMBEDDED_SERVICE_PORT = EMBEDDED_PUBSUB_SERVICE.getPort();

    final PubSub pubSubService = EMBEDDED_PUBSUB_SERVICE.getOptions()
        .toBuilder()
        .setProjectId(PROJECT_ID)
        .setHost(Joiner.on(':').join(getServiceHost(), getServicePort()))
        .build()
        .getService();

    EMBEDDED_PUBSUB_SERVICE.start();

    pubSubService.create(TopicInfo.of(TOPIC_NAME));
    SUBSCRIPTION = pubSubService.create(SubscriptionInfo.of(TOPIC_NAME, SUBSCRIPTION_NAME));

    LOGGER.info(String.format("       topics: %s", pubSubService.listTopics().getValues()));
    LOGGER.info(String.format("subscriptions: %s", pubSubService.listSubscriptions().getValues()));
  }

  @AfterClass
  public static void shutdownPubSub() throws InterruptedException, TimeoutException, IOException {
    EMBEDDED_PUBSUB_SERVICE.stop(Duration.standardSeconds(10L));
  }

  public static String getServiceHost() {
    return EMBEDDED_SERVICE_HOST;
  }

  public static int getServicePort() {
    return EMBEDDED_SERVICE_PORT;
  }

  public static Subscription getServiceSubscription() {
    return SUBSCRIPTION;
  }
}
