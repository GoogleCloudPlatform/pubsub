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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;

class PubsubOffset extends Offset implements PartitionOffset {
  final List<String> ackIds = new ArrayList<>();

  @Override
  public String json() {
    Gson gson = new Gson();
    return gson.toJson(ackIds);
  }

  static PubsubOffset fromJson(String json) {
    PubsubOffset offset = new PubsubOffset();
    Gson gson = new Gson();
    Collection<String> ackIds = gson.fromJson(json, new TypeToken<Collection<String>>(){}.getType());
    offset.ackIds.addAll(ackIds);
    return offset;
  }
}
