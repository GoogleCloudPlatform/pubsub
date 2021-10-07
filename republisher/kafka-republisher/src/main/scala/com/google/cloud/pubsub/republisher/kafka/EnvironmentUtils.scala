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

package com.google.cloud.pubsub.republisher.kafka

import org.apache.kafka.common.Node

object EnvironmentUtils {
  val KAFKA_ROUTING_HOST = "KAFKA_ROUTING_HOST"
  val KAFKA_ROUTING_PORT = "KAFKA_ROUTING_PORT"
  val ROUTING_NODE = new Node(0, sys.env(KAFKA_ROUTING_HOST), sys.env(KAFKA_ROUTING_PORT).toInt)
}
