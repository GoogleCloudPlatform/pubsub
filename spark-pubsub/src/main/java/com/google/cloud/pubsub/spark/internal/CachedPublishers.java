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

import com.google.cloud.pubsub.v1.Publisher;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;

/** Cached {@link Publisher}s to reuse publisher of same settings in the same task. */
public class CachedPublishers {
  @GuardedBy("this")
  private final Map<WriteDataSourceOptions, Publisher> publishers = new HashMap<>();

  public synchronized Publisher getOrCreate(
      WriteDataSourceOptions writeOptions) {
    return publishers.computeIfAbsent(writeOptions, WriteDataSourceOptions::newPublisher);
  }
}
