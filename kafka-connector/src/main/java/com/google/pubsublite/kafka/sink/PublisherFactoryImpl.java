package com.google.pubsublite.kafka.sink;

import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.ProjectPath;
import com.google.cloud.pubsublite.PublishMetadata;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.PubsubContext.Framework;
import com.google.cloud.pubsublite.internal.wire.RoutingPublisherBuilder;
import com.google.cloud.pubsublite.internal.wire.SinglePartitionPublisherBuilder;
import java.util.Map;
import org.apache.kafka.common.config.ConfigValue;

class PublisherFactoryImpl implements PublisherFactory {

  private static final Framework FRAMEWORK = Framework.of("KAFKA_CONNECT");

  @Override
  public Publisher<PublishMetadata> newPublisher(Map<String, String> params) {
    Map<String, ConfigValue> config = ConfigDefs.config().validateAll(params);
    RoutingPublisherBuilder.Builder builder = RoutingPublisherBuilder.newBuilder();
    TopicPath topic = TopicPath.newBuilder()
        .setProject(ProjectPath.parse("projects/" + config.get(ConfigDefs.PROJECT_FLAG).value()).project())
        .setLocation(CloudZone.parse(config.get(ConfigDefs.LOCATION_FLAG).value().toString()))
        .setName(TopicName.of(config.get(ConfigDefs.TOPIC_NAME_FLAG).value().toString())).build();
    builder.setTopic(topic);
    builder.setPublisherFactory(
        partition -> SinglePartitionPublisherBuilder.newBuilder().setTopic(topic)
            .setPartition(partition).setContext(
                PubsubContext.of(FRAMEWORK)).build());
    return builder.build();
  }
}
