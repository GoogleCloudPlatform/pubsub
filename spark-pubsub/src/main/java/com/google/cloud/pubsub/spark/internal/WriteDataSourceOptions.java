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

package com.google.cloud.pubsub.spark.internal;

import com.google.api.gax.rpc.ApiException;
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

@AutoValue
public abstract class WriteDataSourceOptions implements Serializable {

  @Nullable
  public abstract String credentialsKey();

  public abstract TopicName topic();

  public static Builder builder() {
    return new AutoValue_WriteDataSourceOptions.Builder().setCredentialsKey(null);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setCredentialsKey(String credentialsKey);

    public abstract Builder setTopic(TopicName topic);

    public abstract WriteDataSourceOptions build();
  }

  public static WriteDataSourceOptions fromSparkDataSourceOptions(DataSourceOptions options) {
    if (!options.get(Constants.TOPIC_CONFIG_KEY).isPresent()) {
      throw new IllegalArgumentException(Constants.TOPIC_CONFIG_KEY + " is required.");
    }

    Builder builder = builder();
    String topicPathVal = options.get(Constants.TOPIC_CONFIG_KEY).get();
    try {
      builder.setTopic(TopicName.parse(topicPathVal));
    } catch (ApiException e) {
      throw new IllegalArgumentException("Unable to parse topic path " + topicPathVal, e);
    }
    options.get(Constants.CREDENTIALS_KEY_CONFIG_KEY).ifPresent(builder::setCredentialsKey);
    return builder.build();
  }

  Publisher newPublisher() {
    try {
      return Publisher.newBuilder(topic())
          .setCredentialsProvider(new CpsCredentialsProvider(credentialsKey())).build();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
