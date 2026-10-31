/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.pubsub.flink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.protobuf.util.JsonFormat;
import com.google.pubsub.flink.internal.source.reader.PubSubAckTracker;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Broker-boundary check that suppressed redeliveries keep every delivery handle until the
 * checkpoint, instead of NACKing superseded handles and triggering a redelivery loop.
 *
 * <p>Runs against the Pub/Sub emulator by default. Set PUBSUB_TEST_PROJECT to a disposable cloud
 * project to run against the real service instead, using ADC. Emulator results do not establish
 * cloud semantics.
 */
public class PubSubRedeliverySuppressionIT {
  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  private String base;
  private String project;
  private String emulator;
  private GoogleCredentials credentials;

  @Before
  public void setUp() throws Exception {
    project = System.getenv("PUBSUB_TEST_PROJECT");
    if (project == null) {
      emulator = PubSubEmulator.endpoint();
      project = "redelivery-test";
      base = "http://" + emulator + "/v1/";
    } else {
      assertTrue(project.matches("[a-z][a-z0-9-]{4,61}[a-z0-9]"));
      base = "https://pubsub.googleapis.com/v1/";
      credentials =
          GoogleCredentials.getApplicationDefault()
              .createScoped("https://www.googleapis.com/auth/pubsub");
    }
  }

  @Test(timeout = 120000)
  public void standardSubscription_suppressedRedeliveriesWaitForCheckpointWithoutLooping()
      throws Exception {
    verifyDeferredRedeliveryHandles(false);
  }

  @Test(timeout = 120000)
  public void exactlyOnceSubscription_suppressedRedeliveriesWaitForCheckpointWithoutLooping()
      throws Exception {
    Assume.assumeTrue("Exactly-once delivery requires the real service", emulator == null);
    verifyDeferredRedeliveryHandles(true);
  }

  /**
   * Forces a new delivery by expiring its predecessor's lease. No handle is resolved until the
   * checkpoint: NACKing the superseded handle can trigger another immediate delivery on standard
   * subscriptions. The current-handle NACK positive control verifies broker redelivery is active.
   * This deterministic lease-expiry case does not simulate a StreamingPull reconnect duplicate
   * while its predecessor's lease remains valid. Emulator results do not establish cloud behavior.
   */
  private void verifyDeferredRedeliveryHandles(boolean exactlyOnce) throws Exception {
    String suffix = "superseded-nack-" + UUID.randomUUID();
    String topic = "projects/" + project + "/topics/" + suffix;
    String subscription = "projects/" + project + "/subscriptions/" + suffix;
    boolean passed = false;
    try {
      api("PUT", topic, "{}");
      api(
          "PUT",
          subscription,
          "{\"topic\":\""
              + topic
              + "\",\"ackDeadlineSeconds\":120,\"enableExactlyOnceDelivery\":"
              + exactlyOnce
              + "}");
      PublishResponse.Builder publication = PublishResponse.newBuilder();
      JsonFormat.parser()
          .merge(
              api("POST", topic + ":publish", "{\"messages\":[{\"data\":\"QQ==\"}]}"), publication);
      String messageId = publication.getMessageIds(0);
      ReceivedMessage first = nextDelivery(subscription, messageId);
      PubSubAckTracker tracker = new PubSubAckTracker();
      AtomicInteger nackRequests = new AtomicInteger();
      List<Integer> ackStatuses = new ArrayList<>();
      assertTrue(
          tracker.addPendingAck(
              messageId,
              redeliveryConsumer(
                  subscription, first.getAckId(), exactlyOnce, nackRequests, ackStatuses)));
      tracker.stagePendingAck(messageId);
      tracker.addCheckpoint(1);
      api(
          "POST",
          subscription + ":modifyAckDeadline",
          "{\"ackIds\":[\"" + first.getAckId() + "\"],\"ackDeadlineSeconds\":1}");
      Thread.sleep(1100);
      ReceivedMessage second = nextDelivery(subscription, messageId);
      assertTrue("Redelivery must have a new ACK ID", !first.getAckId().equals(second.getAckId()));
      assertTrue(
          !tracker.addPendingAck(
              messageId,
              redeliveryConsumer(
                  subscription, second.getAckId(), exactlyOnce, nackRequests, ackStatuses)));
      assertTrue(ackStatuses.isEmpty());
      assertNoDelivery(subscription, Duration.ofSeconds(3));
      assertEquals(0, nackRequests.get());

      // Positive control: NACKing the CURRENT handle must cause another delivery. This proves
      // that the observation uses an active subscription and a functional NACK path.
      api(
          "POST",
          subscription + ":modifyAckDeadline",
          "{\"ackIds\":[\"" + second.getAckId() + "\"],\"ackDeadlineSeconds\":0}");
      ReceivedMessage third = nextDelivery(subscription, messageId);
      assertTrue(
          !tracker.addPendingAck(
              messageId,
              redeliveryConsumer(
                  subscription, third.getAckId(), exactlyOnce, nackRequests, ackStatuses)));
      assertTrue(ackStatuses.isEmpty());
      assertNoDelivery(subscription, Duration.ofSeconds(3));
      assertEquals(0, nackRequests.get());
      tracker.notifyCheckpointComplete(1);
      tracker.nackAll();
      assertEquals(
          "Completion must not NACK an acknowledged current handle", 0, nackRequests.get());
      assertEquals(3, ackStatuses.size());
      assertEquals(
          "The current handle must be acknowledged successfully",
          Integer.valueOf(200),
          ackStatuses.get(0));
      assertNoDelivery(subscription, Duration.ofSeconds(2));
      System.out.println(
          "SUPERSEDED_NACK_REPRO exactlyOnce="
              + exactlyOnce
              + " forcedDeliveries=3 supersededNacks="
              + nackRequests.get()
              + " unexpectedDeliveries=0 checkpointAcknowledged=true");
      passed = true;
    } finally {
      if (passed || emulator != null) {
        delete(subscription);
        delete(topic);
      } else {
        System.out.println(
            "SUPERSEDED_NACK_REPRO retained topic=" + topic + " subscription=" + subscription);
      }
    }
  }

  private ReceivedMessage nextDelivery(String subscription, String expectedId) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (System.nanoTime() < deadline) {
      List<ReceivedMessage> deliveries = pull(subscription);
      if (!deliveries.isEmpty()) {
        assertEquals(1, deliveries.size());
        assertEquals(expectedId, deliveries.get(0).getMessage().getMessageId());
        return deliveries.get(0);
      }
      Thread.sleep(50);
    }
    throw new AssertionError("Expected delivery not received within 30 seconds");
  }

  private void assertNoDelivery(String subscription, Duration interval) throws Exception {
    long deadline = System.nanoTime() + interval.toNanos();
    while (System.nanoTime() < deadline) {
      // Finish each pull instead of cancelling at the observation boundary. A cancelled REST pull
      // can otherwise lease the positive-control delivery after the client has stopped waiting.
      // The HTTP request timeout is 20s, well below the current delivery's 120s lease.
      assertTrue(
          "Unexpected delivery before its current lease expired", pull(subscription).isEmpty());
      Thread.sleep(50);
    }
  }

  private AckReplyConsumer redeliveryConsumer(
      String subscription,
      String ackId,
      boolean exactlyOnce,
      AtomicInteger nackRequests,
      List<Integer> ackStatuses) {
    return new AckReplyConsumer() {
      @Override
      public void ack() {
        try {
          int status = sendAck(subscription, ackId);
          ackStatuses.add(status);
          assertTrue(
              "ACK failed: HTTP " + status,
              status >= 200 && status < 300 || exactlyOnce && status == 400);
        } catch (Exception failure) {
          throw new IllegalStateException("Acknowledgement request failed", failure);
        }
      }

      @Override
      public void nack() {
        nackRequests.incrementAndGet();
        try {
          int status =
              request(
                      "POST",
                      subscription + ":modifyAckDeadline",
                      "{\"ackIds\":[\"" + ackId + "\"],\"ackDeadlineSeconds\":0}")
                  .statusCode();
          assertTrue(
              "Superseded NACK failed: HTTP " + status,
              status >= 200 && status < 300 || exactlyOnce && status == 400);
        } catch (Exception failure) {
          throw new IllegalStateException("Negative acknowledgement request failed", failure);
        }
      }
    };
  }

  private List<ReceivedMessage> pull(String subscription) throws Exception {
    PullResponse.Builder response = PullResponse.newBuilder();
    JsonFormat.parser()
        .merge(
            api("POST", subscription + ":pull", "{\"maxMessages\":10,\"returnImmediately\":true}"),
            response);
    return response.getReceivedMessagesList();
  }

  private int sendAck(String subscription, String ackId) throws Exception {
    return request("POST", subscription + ":acknowledge", "{\"ackIds\":[\"" + ackId + "\"]}")
        .statusCode();
  }

  private HttpResponse<String> request(String method, String path, String body) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create(base + path))
            .timeout(Duration.ofSeconds(20))
            .header("Content-Type", "application/json");
    if (credentials != null) {
      credentials.refreshIfExpired();
      request.header("Authorization", "Bearer " + credentials.getAccessToken().getTokenValue());
    }
    return http.send(
        request.method(method, HttpRequest.BodyPublishers.ofString(body)).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private String api(String method, String path, String body) throws Exception {
    HttpResponse<String> response = request(method, path, body);
    // Never include response bodies: an error may repeat an acknowledgement token.
    assertTrue(
        method + " " + path + ": HTTP " + response.statusCode(),
        response.statusCode() >= 200 && response.statusCode() < 300);
    return response.body();
  }

  private void delete(String path) throws Exception {
    int status = request("DELETE", path, "").statusCode();
    assertTrue(
        "Cleanup " + path + ": HTTP " + status, status == 404 || (status >= 200 && status < 300));
  }
}
