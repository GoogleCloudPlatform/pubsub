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
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

@AutoValue
public abstract class ReadDataSourceOptions implements Serializable {
  @Nullable
  public abstract String credentialsKey();

  public abstract SubscriptionName subscription();

  public abstract int readShards();

  public static Builder builder() {
    return new AutoValue_ReadDataSourceOptions.Builder().setCredentialsKey(null);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setCredentialsKey(String credentialsKey);

    public abstract Builder setSubscription(SubscriptionName subscription);

    public abstract Builder setReadShards(int readShards);

    public abstract ReadDataSourceOptions build();
  }

  public static ReadDataSourceOptions fromSparkDataSourceOptions(DataSourceOptions options) {
    if (!options.get(Constants.SUBSCRIPTION_CONFIG_KEY).isPresent()) {
      throw new IllegalArgumentException(Constants.SUBSCRIPTION_CONFIG_KEY + " is required.");
    }

    Builder builder = builder();
    String pathVal = options.get(Constants.SUBSCRIPTION_CONFIG_KEY).get();
    try {
      builder.setSubscription(SubscriptionName.parse(pathVal));
    } catch (ApiException e) {
      throw new IllegalArgumentException("Unable to parse subscription path " + pathVal, e);
    }
    options.get(Constants.CREDENTIALS_KEY_CONFIG_KEY).ifPresent(builder::setCredentialsKey);
    builder.setReadShards(options.getInt(Constants.READ_SHARDS_CONFIG_KEY, Constants.DEFAULT_READ_SHARDS));
    return builder.build();
  }

  private SubscriberStub newStub() {
    try {
      return SubscriberStubSettings.newBuilder()
          .setCredentialsProvider(new CpsCredentialsProvider(credentialsKey())).build().createStub();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public Committer getCommitter() {
    return new CommitterImpl(newStub(), subscription());
  }

  public PullSubscriber getPullSubscriber() {
    return new PullSubscriberImpl(newStub(), subscription());
  }
}
