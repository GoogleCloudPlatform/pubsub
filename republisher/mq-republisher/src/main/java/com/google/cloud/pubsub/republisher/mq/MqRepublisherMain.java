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

import com.google.cloud.pubsub.republisher.PublisherCaches;
import com.google.cloud.pubsublite.internal.wire.PubsubContext.Framework;
import com.google.common.base.Strings;
import com.hivemq.configuration.service.InternalConfigurations;
import com.hivemq.embedded.EmbeddedExtension;
import com.hivemq.embedded.EmbeddedHiveMQ;
import com.hivemq.embedded.EmbeddedHiveMQBuilder;
import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.parameter.ExtensionStartInput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStartOutput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStopInput;
import com.hivemq.extension.sdk.api.parameter.ExtensionStopOutput;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.migration.meta.PersistenceType;
import java.nio.file.Paths;

public class MqRepublisherMain {
  private static final String HIVE_MQ_CONFIG_FOLDER = "HIVE_MQ_CONFIG_FOLDER";

  public static void main(String[] args) throws Exception {
    String config = System.getenv(HIVE_MQ_CONFIG_FOLDER);
    if (Strings.isNullOrEmpty(config)) {
      throw new Exception("HIVE_MQ_CONFIG_FOLDER must be set.");
    }
    final EmbeddedHiveMQBuilder embeddedHiveMQBuilder = EmbeddedHiveMQ.builder()
        .withConfigurationFolder(Paths.get(config))
        .withEmbeddedExtension(
            EmbeddedExtension.builder()
                .withId("PubSubExtension")
                .withName("PubSub Republisher")
                .withVersion("1.0.0")
                .withPriority(0)
                .withStartPriority(1000)
                .withAuthor("Pub/Sub Team")
                .withExtensionMain(new PubSubExtensionMain())
                .build());
    try (final EmbeddedHiveMQ hiveMQ = embeddedHiveMQBuilder.build()) {
      // We disable rocksdb since it isn't needed.
      InternalConfigurations.PAYLOAD_PERSISTENCE_TYPE.set(PersistenceType.FILE);
      InternalConfigurations.RETAINED_MESSAGE_PERSISTENCE_TYPE.set(PersistenceType.FILE);
      hiveMQ.start().join();
    }
  }

  private static final class PubSubExtensionMain implements ExtensionMain {
    @Override
    public void extensionStart(ExtensionStartInput extensionStartInput, ExtensionStartOutput extensionStartOutput) {
      final RepublishInterceptor publishInterceptor =
          new RepublishInterceptor(PublisherCaches.create(Framework.of("MQ_REPUBLISHER")));
      final StopSubscribeInterceptor subscribeInterceptor = new StopSubscribeInterceptor();
      Services.initializerRegistry().setClientInitializer((initializerInput, clientContext) -> {
        clientContext.addPublishInboundInterceptor(publishInterceptor);
        clientContext.addSubscribeInboundInterceptor(subscribeInterceptor);
      });
    }

    @Override
    public void extensionStop(ExtensionStopInput extensionStopInput, ExtensionStopOutput extensionStopOutput) {}
  }
}
