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
package com.google.cloud.pubsub.archetype;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A subscriber that classifies every processing failure before deciding what to do with the
 * message. This is the piece that stops poison messages from bouncing forever.
 *
 * <p>The redelivery machinery of Pub/Sub (nack &rarr; retry &rarr; dead-letter) is <b>only</b> for
 * transient failures. Deterministic failures must never ride it: retrying them is pointless and, at
 * scale, self-inflicts a redelivery storm. Hence the three-way decision:
 *
 * <table border="1">
 *   <caption>Delivery decision table</caption>
 *   <tr><th>Downstream outcome</th><th>Nature</th><th>Action</th></tr>
 *   <tr><td>Accepted (functional ACK)</td><td>Success</td><td>{@code ack()} &mdash; done</td></tr>
 *   <tr><td>Transient (timeout, 5xx, connection)</td><td>Retryable</td>
 *       <td>{@code nack()} &mdash; redeliver with backoff, eventually dead-letter</td></tr>
 *   <tr><td>Functional reject (contract / data)</td><td>Deterministic</td>
 *       <td>{@code ack()} + republish to quarantine &mdash; never retried</td></tr>
 * </table>
 */
public final class ClassifyingReceiver implements MessageReceiver {

  private static final Logger logger = Logger.getLogger(ClassifyingReceiver.class.getName());

  /** How a (simulated or real) downstream delivery ended. */
  public enum DeliveryOutcome {
    ACCEPTED,
    TRANSIENT_FAILURE,
    FUNCTIONAL_REJECT
  }

  /** Pluggable downstream. Real deployments wire this to the actual receiving system. */
  public interface Downstream {
    DeliveryOutcome deliver(PubsubMessage message);
  }

  private final Downstream downstream;
  private final Publisher quarantinePublisher;

  public ClassifyingReceiver(Downstream downstream, Publisher quarantinePublisher) {
    this.downstream = downstream;
    this.quarantinePublisher = quarantinePublisher;
  }

  @Override
  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
    DeliveryOutcome outcome;
    try {
      outcome = downstream.deliver(message);
    } catch (RuntimeException unexpected) {
      // Unknown exceptions are treated as transient: better to retry than to silently drop.
      logger.log(Level.WARNING, "Downstream threw; treating as transient", unexpected);
      consumer.nack();
      return;
    }

    switch (outcome) {
      case ACCEPTED:
        consumer.ack();
        return;

      case TRANSIENT_FAILURE:
        // Retryable: let Pub/Sub redeliver with backoff (and eventually dead-letter it).
        consumer.nack();
        return;

      case FUNCTIONAL_REJECT:
      default:
        // Deterministic: it will fail identically forever. Remove it from the retry loop
        // (ack) and park it in quarantine with its reason for a human/spec decision.
        quarantine(message);
        consumer.ack();
        return;
    }
  }

  private void quarantine(PubsubMessage original) {
    PubsubMessage tagged =
        PubsubMessage.newBuilder()
            .setData(original.getData())
            .putAllAttributes(original.getAttributesMap())
            .putAttributes("quarantine-reason", "FUNCTIONAL_REJECT")
            .putAttributes("original-message-id", original.getMessageId())
            .build();
    try {
      quarantinePublisher.publish(tagged).get();
    } catch (Exception e) {
      // If we cannot even quarantine, fall back to nack so the message is not lost.
      logger.log(Level.SEVERE, "Failed to publish to quarantine topic", e);
      throw new RuntimeException(e);
    }
  }

  /** Convenience for callers that hold raw bytes rather than a PubsubMessage. */
  static PubsubMessage message(String data) {
    return PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(data)).build();
  }
}
