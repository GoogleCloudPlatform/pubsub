// Copyright 2016 Google Inc.
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

package com.google.pubsub.flic.controllers;

import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

/**
 * Keeps track of the parameters that define a client.
 */
public class ClientParams {

  public final String topic;
  public final String subscription;
  public final Client.ClientType clientType;

  public ClientParams(Client.ClientType clientType, String subscription) {
    this(clientType, null, subscription);
  }

  public ClientParams(Client.ClientType clientType, String topic, String subscription) {
    this.clientType = clientType;
    if (StringUtils.isBlank(topic)) {
      this.topic = Client.TOPIC_PREFIX + Client.getTopicSuffix(clientType);
    } else {
      this.topic = topic;
    }
    this.subscription = subscription;
  }

  /**
   * @return the clientType
   */
  public Client.ClientType getClientType() {
    return clientType;
  }

  public String getTopic() {
    return topic;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClientParams that = (ClientParams) o;
    return Objects.equals(topic, that.topic) &&
        Objects.equals(subscription, that.subscription) &&
        clientType == that.clientType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, subscription, clientType);
  }
}
