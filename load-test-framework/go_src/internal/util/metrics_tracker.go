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

package util

import (
	"google.com/cloud_pubsub_loadtest/internal/genproto"
	"go/types"
	"math"
	"sync/atomic"
)

type MessageAndDuration struct {
	PublisherId    int64
	SequenceNumber int32
	LatencyMs      int
}

type MetricsTracker interface {
	Put(messageAndDuration MessageAndDuration)
	PutFailure()
	Check() genproto.CheckResponse
}

type metricsTrackerImpl struct {
	// Whether to track message ids
	trackIds bool
	// The current response value
	response genproto.CheckResponse
	// The channel to put values in
	putChannel chan MessageAndDuration
	// The count of failed requests.  Only modify atomically
	failedRequests int64
	// The channel for check requests
	checkRequestChannel chan types.Nil
	// The channel to send check responses to
	checkResponseChannel chan genproto.CheckResponse
}

func (mt *metricsTrackerImpl) Put(messageAndDuration MessageAndDuration) {
	mt.putChannel <- messageAndDuration
}

func (mt *metricsTrackerImpl) PutFailure() {
	atomic.AddInt64(&mt.failedRequests, 1)
}

func (mt *metricsTrackerImpl) Check() genproto.CheckResponse {
	mt.checkRequestChannel <- types.Nil{}
	return <-mt.checkResponseChannel
}

func logBase(value float64, base float64) float64 {
	return math.Log2(value) / math.Log2(float64(base))
}

func bucketFor(latencyMs int) int {
	rawBucket := math.Floor(logBase(float64(latencyMs), 1.5))
	return int(math.Max(0, rawBucket))
}

// The size of 100 10MB batches of 1KB messages
const putChannelBufferSize = 1000000

func NewMetricsTracker(trackIds bool) MetricsTracker {
	mt := &metricsTrackerImpl{
		trackIds:             trackIds,
		putChannel:           make(chan MessageAndDuration, putChannelBufferSize),
		checkRequestChannel:  make(chan types.Nil),
		checkResponseChannel: make(chan genproto.CheckResponse),
	}
	// updater
	go func() {
		for {
			select {
			case messageAndDuration := <-mt.putChannel:
				bucket := bucketFor(messageAndDuration.LatencyMs)
				for len(mt.response.BucketValues) <= bucket {
					mt.response.BucketValues = append(mt.response.BucketValues, 0)
				}
				mt.response.BucketValues[bucket]++
				if trackIds {
					mt.response.ReceivedMessages = append(
						mt.response.ReceivedMessages, &genproto.MessageIdentifier{
							PublisherClientId: messageAndDuration.PublisherId,
							SequenceNumber:    messageAndDuration.SequenceNumber,
						})
				}
			case <-mt.checkRequestChannel:
				mt.response.Failed = atomic.SwapInt64(&mt.failedRequests, 0)
				mt.checkResponseChannel <- mt.response
				mt.response = genproto.CheckResponse{}
			}
		}
	}()
	return mt
}
