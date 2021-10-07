/*
 * Copyright 2021 Google LLC
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

package com.google.cloud.pubsub.republisher.mq;

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.republisher.PublisherCache;
import com.google.cloud.pubsub.v1.PublisherInterface;
import com.google.cloud.pubsublite.internal.wire.SystemExecutors;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.flogger.GoogleLogger;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.hivemq.extension.sdk.api.async.Async;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundOutput;
import com.hivemq.extension.sdk.api.packets.publish.AckReasonCode;
import com.hivemq.extension.sdk.api.packets.publish.PublishPacket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class RepublishInterceptor implements PublishInboundInterceptor {
  // Content type is attached to this attribute.
  private static final String CONTENT_TYPE = "mq-republisher-content-type";

  // Cloud Pub/Sub does not allow publishing empty payloads.
  private static final ByteBuffer EMPTY_BUFFER =
      ByteBuffer.wrap("<empty>".getBytes(StandardCharsets.UTF_8));

  private final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final PublisherCache cache;

  RepublishInterceptor(PublisherCache cache) {
    this.cache = cache;
  }

  @Override
  public void onInboundPublish(
      PublishInboundInput publishInboundInput, PublishInboundOutput publishInboundOutput) {
    PublishPacket packet = publishInboundInput.getPublishPacket();
    PublisherInterface publisher;
    try {
      publisher = cache.getPublisher(packet.getTopic());
    } catch (Throwable t) {
      logger.atWarning().withCause(t).log("Failed to create publisher:\n{}", packet);
      publishInboundOutput.preventPublishDelivery(
          AckReasonCode.IMPLEMENTATION_SPECIFIC_ERROR,
          "Failed to create publisher: " + t.getMessage());
      return;
    }
    final Async<PublishInboundOutput> asyncOutput =
        publishInboundOutput.async(Duration.ofMinutes(1));
    ApiFutures.addCallback(
        publisher.publish(parse(packet)),
        new ApiFutureCallback<String>() {
          @Override
          public void onFailure(Throwable t) {
            logger.atWarning().withCause(t).log("Failed to publish message:\n{}", packet);
            asyncOutput
                .getOutput()
                .preventPublishDelivery(
                    AckReasonCode.IMPLEMENTATION_SPECIFIC_ERROR,
                    "Failed to publish: " + t.getMessage());
            asyncOutput.resume();
          }

          @Override
          public void onSuccess(String s) {
            asyncOutput.getOutput().preventPublishDelivery(); // Return success but don't persist.
            asyncOutput.resume();
          }
        },
        SystemExecutors.getFuturesExecutor());
  }

  @VisibleForTesting
  static PubsubMessage parse(PublishPacket packet) {
    PubsubMessage.Builder builder = PubsubMessage.newBuilder();
    ByteString payload = ByteString.copyFrom(packet.getPayload().orElse(EMPTY_BUFFER));
    if (payload.isEmpty()) payload = ByteString.copyFrom(EMPTY_BUFFER);
    builder.setData(payload);
    ImmutableListMultimap.Builder<String, String> mapBuilder = ImmutableListMultimap.builder();
    packet.getContentType().ifPresent(val -> mapBuilder.put(CONTENT_TYPE, val));
    packet
        .getUserProperties()
        .asList()
        .forEach(prop -> mapBuilder.put(prop.getName(), prop.getValue()));
    // Format attributes as comma separated http headers
    mapBuilder
        .build()
        .asMap()
        .forEach((key, values) -> builder.putAttributes(key, String.join(", ", values)));
    return builder.build();
  }
}
