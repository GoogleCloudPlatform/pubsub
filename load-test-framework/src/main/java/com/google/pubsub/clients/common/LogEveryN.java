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

package com.google.pubsub.clients.common;

import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

public class LogEveryN {
  private final Logger logger;
  private final AtomicInteger counter = new AtomicInteger(0);
  private final int n;

  public LogEveryN(Logger logger, int n) {
    this.logger = logger;
    this.n = n;
  }

  public void error(String toLog) {
    int previous = counter.getAndUpdate(existing -> (existing + 1) % n);
    if (previous == 0) {
      logger.error(toLog);
    }
  }
}
