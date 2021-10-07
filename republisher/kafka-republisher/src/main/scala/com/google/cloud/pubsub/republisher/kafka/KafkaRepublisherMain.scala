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

import com.google.cloud.pubsub.republisher.PublisherCaches
import com.google.cloud.pubsub.republisher.kafka.EnvironmentUtils.ROUTING_NODE
import com.google.cloud.pubsublite.internal.wire.PubsubContext.Framework
import com.google.common.collect.ImmutableMap
import kafka.cluster.EndPoint
import kafka.network.SocketServer
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig.{NodeIdProp, NumIoThreadsProp, NumNetworkThreadsProp, ZkConnectProp}
import kafka.server.{ApiVersionManager, BrokerFeatures, FinalizedFeatureCache, KafkaConfig, KafkaRequestHandlerPool}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.Time

object KafkaRepublisherMain {
  val PORT = 9092

  def main(args: Array[String]): Unit = {
    val configBuilder = ImmutableMap.builder[String, String]()
    configBuilder.put(NodeIdProp, ROUTING_NODE.id().toString)
    configBuilder.put(NumNetworkThreadsProp, "1")
    configBuilder.put(NumIoThreadsProp, "1")
    configBuilder.put(ZkConnectProp, "bad")
    val config = new KafkaConfig(configBuilder.build(), true, None)
    val metrics = new Metrics()
    val credentialProvider = new CredentialProvider(
      ScramMechanism.mechanismNames,
      new DelegationTokenCache(ScramMechanism.mechanismNames))
    val features = BrokerFeatures.createDefault()
    val featureCache = new FinalizedFeatureCache(features)
    val apiVersionManager = ApiVersionManager(
      ListenerType.BROKER, config, None, features, featureCache)
    val server = new SocketServer(
      config, metrics, Time.SYSTEM, credentialProvider, apiVersionManager)
    val protocol = SecurityProtocol.PLAINTEXT
    server.startup(
      startProcessingRequests = false, controlPlaneListener = None,
      dataPlaneListeners = List(
        new EndPoint(host = "0.0.0.0", port = PORT,
          ListenerName.forSecurityProtocol(protocol), protocol)))
    val processor = new ProducerApiRequestHandler(server.dataPlaneRequestChannel, PublisherCaches.create(Framework.of("KAFKA_REPUBLISHER")))
    val handlers = new KafkaRequestHandlerPool(ROUTING_NODE.id(),
      server.dataPlaneRequestChannel, processor, Time.SYSTEM,
      config.numIoThreads, s"${SocketServer.DataPlaneMetricPrefix}RequestHandlerAvgIdlePercent",
      SocketServer.DataPlaneThreadPrefix)
    server.startProcessingRequests()
    System.err.println("Started.")
    Thread.sleep(Long.MaxValue)
  }
}
