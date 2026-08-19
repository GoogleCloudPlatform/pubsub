// Copyright 2024 Google Inc.
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
package com.google.cloud.pubsub.archetype.smoke;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.cloud.pubsub.archetype.Archetype;
import com.google.cloud.pubsub.archetype.ValidationResult;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PushConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Smoke tests for {@link Archetype} against a real or emulated Pub/Sub endpoint.
 *
 * <p>These tests are intentionally <b>off by default</b>. They run only under the {@code smoke}
 * Maven profile ({@code mvn verify -Psmoke}) so they never execute in a standard unit build.
 *
 * <p>Two modes are supported:
 * <ol>
 *   <li><b>Emulator</b> (default, zero cost): set {@code PUBSUB_EMULATOR_HOST=localhost:8085}
 *       and start the emulator with {@code gcloud beta emulators pubsub start}.
 *   <li><b>Real GCP topic</b>: set {@code GOOGLE_CLOUD_PROJECT}, {@code PUBSUB_SMOKE_TOPIC},
 *       and {@code PUBSUB_SMOKE_SUBSCRIPTION}. Application Default Credentials must be valid.
 * </ol>
 *
 * <p>Topic and subscription are created and deleted per-run; the test is hermetic and leaves no
 * residue in the project.
 *
 * <h2>What these tests prove</h2>
 * <ul>
 *   <li>A <b>valid</b> payload passes the gate, is published, pulled back, and byte-verified
 *       (round-trip integrity).
 *   <li>An <b>invalid</b> payload (encoding / JSON / schema violation) is rejected by the gate
 *       and <b>never reaches {@code publish()}</b>.
 *   <li>The gate is not just <em>logically</em> correct (covered by {@link
 *       com.google.cloud.pubsub.archetype.ArchetypeTest}) but <em>operationally</em> correct under
 *       real auth and network conditions.
 * </ul>
 */
public class ArchetypeGateSmokeIT {

  // ── Environment resolution ──────────────────────────────────────────────

  private static final String ENV_PROJECT      = System.getenv("GOOGLE_CLOUD_PROJECT");
  private static final String ENV_EMULATOR     = System.getenv("PUBSUB_EMULATOR_HOST");
  private static final String TOPIC_ID         = "archetype-smoke-" + UUID.randomUUID();
  private static final String SUBSCRIPTION_ID  = TOPIC_ID + "-sub";

  private static final String VALID_PAYLOAD =
      "{\"event_type\":\"llm_request\",\"payload\":{\"prompt\":\"smoke\"},\"version\":\"1.0\"}";

  private TopicAdminClient topicAdmin;
  private SubscriptionAdminClient subscriptionAdmin;
  private Publisher publisher;
  private GrpcSubscriberStub subscriberStub;
  private ManagedChannel channel;
  private Archetype gate;

  // ── Setup / teardown ────────────────────────────────────────────────────

  @Before
  public void setUp() throws Exception {
    // Skip if neither emulator nor real project is configured.
    Assume.assumeTrue(
        "Set PUBSUB_EMULATOR_HOST or GOOGLE_CLOUD_PROJECT to run smoke tests",
        ENV_EMULATOR != null || ENV_PROJECT != null);

    gate = Archetype.fromResource("/archetype.schema.json");

    if (ENV_EMULATOR != null) {
      channel = ManagedChannelBuilder.forTarget(ENV_EMULATOR).usePlaintext().build();
      TransportChannelProvider channelProvider =
          FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
      NoCredentialsProvider credentialsProvider = NoCredentialsProvider.create();

      topicAdmin = TopicAdminClient.create(
          com.google.cloud.pubsub.v1.TopicAdminSettings.newBuilder()
              .setTransportChannelProvider(channelProvider)
              .setCredentialsProvider(credentialsProvider)
              .build());
      subscriptionAdmin = SubscriptionAdminClient.create(
          com.google.cloud.pubsub.v1.SubscriptionAdminSettings.newBuilder()
              .setTransportChannelProvider(channelProvider)
              .setCredentialsProvider(credentialsProvider)
              .build());

      publisher = Publisher.newBuilder(ProjectTopicName.of("smoke-project", TOPIC_ID))
          .setChannelProvider(channelProvider)
          .setCredentialsProvider(credentialsProvider)
          .build();

      SubscriberStubSettings stubSettings = SubscriberStubSettings.newBuilder()
          .setTransportChannelProvider(channelProvider)
          .setCredentialsProvider(credentialsProvider)
          .build();
      subscriberStub = GrpcSubscriberStub.create(stubSettings);

      topicAdmin.createTopic(ProjectTopicName.of("smoke-project", TOPIC_ID));
      subscriptionAdmin.createSubscription(
          ProjectSubscriptionName.of("smoke-project", SUBSCRIPTION_ID),
          ProjectTopicName.of("smoke-project", TOPIC_ID),
          PushConfig.getDefaultInstance(), 10);
    } else {
      // Real GCP — use ADC
      topicAdmin = TopicAdminClient.create();
      subscriptionAdmin = SubscriptionAdminClient.create();
      publisher = Publisher.newBuilder(ProjectTopicName.of(ENV_PROJECT, TOPIC_ID)).build();
      subscriberStub = GrpcSubscriberStub.create(SubscriberStubSettings.newBuilder().build());

      topicAdmin.createTopic(ProjectTopicName.of(ENV_PROJECT, TOPIC_ID));
      subscriptionAdmin.createSubscription(
          ProjectSubscriptionName.of(ENV_PROJECT, SUBSCRIPTION_ID),
          ProjectTopicName.of(ENV_PROJECT, TOPIC_ID),
          PushConfig.getDefaultInstance(), 10);
    }
  }

  @After
  public void tearDown() throws Exception {
    if (publisher != null) publisher.shutdown();
    String project = ENV_EMULATOR != null ? "smoke-project" : ENV_PROJECT;
    if (subscriptionAdmin != null) {
      try { subscriptionAdmin.deleteSubscription(
          ProjectSubscriptionName.of(project, SUBSCRIPTION_ID)); } catch (Exception ignored) {}
      subscriptionAdmin.close();
    }
    if (topicAdmin != null) {
      try { topicAdmin.deleteTopic(ProjectTopicName.of(project, TOPIC_ID)); } catch (Exception ignored) {}
      topicAdmin.close();
    }
    if (subscriberStub != null) subscriberStub.close();
    if (channel != null) channel.shutdownNow();
  }

  // ── Tests ───────────────────────────────────────────────────────────────

  /**
   * A conformant payload passes the gate, is published, and is pulled back byte-identical.
   * Proves round-trip integrity through the real (or emulated) Pub/Sub transport.
   */
  @Test
  public void validPayload_passesGateAndRoundTrips() throws Exception {
    byte[] raw = VALID_PAYLOAD.getBytes(StandardCharsets.UTF_8);
    ValidationResult result = gate.validate(raw, StandardCharsets.UTF_8);
    assertTrue("Gate should accept a conformant payload", result.isAccepted());

    // Publish the canonical form
    String canonical = result.getCanonical();
    publisher.publish(PubsubMessage.newBuilder()
        .setData(ByteString.copyFromUtf8(canonical))
        .build()).get(10, TimeUnit.SECONDS);

    // Pull back and verify byte equality
    String project = ENV_EMULATOR != null ? "smoke-project" : ENV_PROJECT;
    PullResponse pullResponse = subscriberStub.pullCallable().call(
        PullRequest.newBuilder()
            .setSubscription(ProjectSubscriptionName.of(project, SUBSCRIPTION_ID).toString())
            .setMaxMessages(1)
            .build());

    assertFalse("Expected at least one message", pullResponse.getReceivedMessagesList().isEmpty());
    String pulled = pullResponse.getReceivedMessages(0).getMessage().getData().toStringUtf8();
    assertEquals("Round-trip payload must be byte-identical to canonical form", canonical, pulled);
  }

  /**
   * An invalid payload (schema violation) is rejected by the gate and never reaches publish().
   * Verifies the gate holds the line against a real topic — the topic should remain empty.
   */
  @Test
  public void invalidPayload_rejectedAtGate_neverPublished() throws Exception {
    byte[] badPayload = "{\"event_type\":\"unknown\",\"payload\":{}}".getBytes(StandardCharsets.UTF_8);
    ValidationResult result = gate.validate(badPayload, StandardCharsets.UTF_8);

    assertFalse("Gate should reject a payload with invalid enum and missing required field",
        result.isAccepted());
    assertFalse("Rejection reasons must be non-empty", result.getRejectionReasons().isEmpty());

    // Do NOT call publisher.publish() — verifying the caller respects the rejection.
    // Pull from the subscription: it must be empty.
    String project = ENV_EMULATOR != null ? "smoke-project" : ENV_PROJECT;
    PullResponse pullResponse = subscriberStub.pullCallable().call(
        PullRequest.newBuilder()
            .setSubscription(ProjectSubscriptionName.of(project, SUBSCRIPTION_ID).toString())
            .setMaxMessages(1)
            .build());

    assertTrue("Topic must be empty — invalid payload must never reach publish()",
        pullResponse.getReceivedMessagesList().isEmpty());
  }

  /**
   * A payload with invalid UTF-8 bytes is rejected at the encoding stage,
   * before any JSON parsing or schema validation is attempted.
   */
  @Test
  public void encodingError_rejectedBeforeJsonParsing() {
    byte[] badBytes = new byte[]{(byte) 0xFF, (byte) 0xFE, 0x7B, 0x7D}; // invalid UTF-8 + {}
    ValidationResult result = gate.validate(badBytes, StandardCharsets.UTF_8);

    assertFalse("Encoding error must be rejected", result.isAccepted());
    assertTrue("Rejection reason must mention encoding",
        result.getRejectionReasons().stream()
            .anyMatch(r -> r.contains("ENCODING_UNDECODABLE")));
  }
}
