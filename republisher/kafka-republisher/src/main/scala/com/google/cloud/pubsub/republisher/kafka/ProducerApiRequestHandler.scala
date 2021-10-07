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

;

import com.google.api.core.{ApiFuture, ApiFutureCallback, ApiFutures}
import com.google.cloud.pubsub.republisher.PublisherCache
import com.google.cloud.pubsublite.internal.wire.SystemExecutors
import com.google.common.collect.ImmutableList
import com.google.common.flogger.GoogleLogger
import kafka.network.RequestChannel
import kafka.network.RequestChannel.Request
import kafka.server.ApiRequestHandler
import kafka.server.RequestLocal
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.ApiVersionsResponseData.{ApiVersionCollection}
import org.apache.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.{ApiVersionsResponse, MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse, RequestUtils}

import scala.jdk.CollectionConverters._

class ProducerApiRequestHandler(val requestChannel: RequestChannel, val publisherCache: PublisherCache) extends ApiRequestHandler {
  override def handle(request: Request, requestLocal: RequestLocal): Unit = {
    try {
      request.header.apiKey match {
        case ApiKeys.PRODUCE => handleProduce(request)
        case ApiKeys.METADATA => handleMetadata(request)
        case ApiKeys.API_VERSIONS => handleApiVersions(request)
        case _ => throw new IllegalArgumentException("Received an unhandled request with key: " + request.header.apiKey + ", request: " + request)
      }
    } catch {
      case e: Throwable =>
        ProducerApiRequestHandler.logger.atSevere().withCause(e).log()
        throw e;
    }
  }

  private def handleProduce(request: Request): Unit = {
    val produceRequest = request.body[ProduceRequest]
    if (RequestUtils.hasTransactionalRecords(produceRequest)) {
      throw new IllegalArgumentException("Received a transactional produce request: " + request)
    }
    val topics = ImmutableList.builder[String]
    val publishFutures = ImmutableList.builder[ApiFuture[String]]
    produceRequest.data.topicData.forEach(topic => {
      topics.add(topic.name)
      val publisher = publisherCache.getPublisher(TranslationUtils.translateTopic(topic.name))
      topic.partitionData.forEach(data => {
        val partition = data.index
        if (partition != 0) {
          throw new IllegalArgumentException("Received a produce request for partitions other than 0: " + request)
        }
        // This caller assumes the type is MemoryRecords and that is true on current serialization
        // We cast the type to avoid causing big change to code base.
        // https://issues.apache.org/jira/browse/KAFKA-10698
        // https://github.com/apache/kafka/blob/1573c707e6a9b0bc14d0e261e958c14d86fff6e4/core/src/main/scala/kafka/server/KafkaApis.scala#L562
        val memoryRecords = data.records.asInstanceOf[MemoryRecords]
        memoryRecords.records().forEach(record => {
          publishFutures.add(publisher.publish(TranslationUtils.translateRecord(record)))
        })
      })
    })
    ApiFutures.addCallback(ApiFutures.allAsList(publishFutures.build), new ApiFutureCallback[Any] {
      override def onFailure(t: Throwable): Unit = {
        ProducerApiRequestHandler.logger.atWarning().withCause(t).log("Publishing failed for request {}", request)
        val response = new PartitionResponse(Errors.UNKNOWN_SERVER_ERROR, t.getMessage)
        requestChannel.closeConnection(request, new ProduceResponse(Map.from(topics.build.asScala.map(topic => new TopicPartition(topic, 0) -> response)).asJava).errorCounts)
      }

      override def onSuccess(v: Any): Unit = {
        val response = new PartitionResponse(Errors.NONE)
        requestChannel.sendResponse(request, new ProduceResponse(Map.from(topics.build.asScala.map(topic => new TopicPartition(topic, 0) -> response)).asJava), None)
      }
    }, SystemExecutors.getFuturesExecutor)
  }

  private def handleMetadata(request: RequestChannel.Request): Unit = {
    val metadataRequest = request.body[MetadataRequest]
    if (metadataRequest.isAllTopics) {
      throw new IllegalArgumentException("Received a metadata request for all topics, which is not supported: " + request)
    }
    val responseTopics = ImmutableList.builder[MetadataResponseTopic]
    metadataRequest.data.topics.forEach(topic => {
      val responseTopic = new MetadataResponseTopic
      responseTopic.setErrorCode(0)
      responseTopic.setName(topic.name)
      responseTopic.setPartitions(List(new MetadataResponsePartition()
        .setPartitionIndex(0)
        .setLeaderId(EnvironmentUtils.ROUTING_NODE.id)
        .setIsrNodes(List(Integer.valueOf(EnvironmentUtils.ROUTING_NODE.id)).asJava)).asJava)
      responseTopics.add(responseTopic)
    })
    val response = MetadataResponse.prepareResponse(true, 0, List(EnvironmentUtils.ROUTING_NODE).asJava, "Pub/Sub Republisher", 0, responseTopics.build, Int.MinValue)
    requestChannel.sendResponse(request, response, None)
  }

  private def handleApiVersions(request: RequestChannel.Request): Unit = {
    val versions = new ApiVersionCollection
    versions.add(ApiVersionsResponse.toApiVersion(ApiKeys.METADATA))
    versions.add(ApiVersionsResponse.toApiVersion(ApiKeys.PRODUCE))
    requestChannel.sendResponse(request, ApiVersionsResponse.createApiVersionsResponse(0, versions), None)
  }
}

object ProducerApiRequestHandler {
  private val logger = GoogleLogger.forEnclosingClass();
}
