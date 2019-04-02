/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.google.pubsub.flic.controllers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientType {
  private static final Logger log = LoggerFactory.getLogger(ClientType.class);

  public enum MessagingType {
    CPS_GCLOUD,
    KAFKA
  }

  public enum Language {
    JAVA,
    PYTHON,
    RUBY,
    GO,
    NODE,
    DOTNET
  }

  public enum MessagingSide {
    PUBLISHER,
    SUBSCRIBER
  }

  public final MessagingType messaging;
  public final Language language;
  public final MessagingSide side;

  public ClientType(MessagingType messaging, Language language, MessagingSide side) {
    if (messaging == MessagingType.KAFKA && language != Language.JAVA) {
      log.error("Passed kafka with a non-java language!");
      System.exit(1);
    }
    this.messaging = messaging;
    this.language = language;
    this.side = side;
  }

  public boolean isCps() {
    return messaging == MessagingType.CPS_GCLOUD;
  }

  public boolean isKafka() {
    return messaging == MessagingType.KAFKA;
  }

  public boolean isPublisher() {
    return side == MessagingSide.PUBLISHER;
  }

  @Override
  public String toString() {
    if (isKafka()) {
      return (messaging + "-" + side).toLowerCase();
    }
    return (messaging.toString().replace("_", "-") + "-" + language + "-" + side).toLowerCase();
  }
}
