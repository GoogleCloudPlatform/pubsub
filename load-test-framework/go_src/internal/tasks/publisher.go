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

package tasks

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/golang/protobuf/ptypes"
	"go/types"
	"google.com/cloud_pubsub_loadtest/internal/flow_control"
	"google.com/cloud_pubsub_loadtest/internal/genproto"
	"google.com/cloud_pubsub_loadtest/internal/util"
	"log"
	"math/rand"
	"strconv"
)

// Start at 100 kB/s/worker
const kStartingPerWorkerBytesPerSecond = float64(100000)

type augmentedPublishResult struct {
	result         *pubsub.PublishResult
	startTimeMs    int64
	sequenceNumber int32
}

type publisherWorker struct {
	publisherId          int64
	metricsTracker       util.MetricsTracker
	stopChannel          <-chan types.Nil
	flowController       flow_control.FlowController
	resultChannel        chan augmentedPublishResult
	collectorStopChannel chan types.Nil
}

// Only the first result is waited on at a time because reflect.Select was determined to be
// limiting throughput.
func (worker *publisherWorker) resultCollector() {
	ctx := context.Background()
	var results []augmentedPublishResult
	for {
		if len(results) > 0 {
			select {
			case <-worker.stopChannel:
				return
			case result := <-worker.resultChannel:
				results = append(results, result)
			case <-results[0].result.Ready(): // a publish future completed
				result := results[0]
				results = results[1:]
				_, err := result.result.Get(ctx)
				if err != nil {
					worker.metricsTracker.PutFailure()
					worker.flowController.InformFinished(false)
					continue
				}
				worker.flowController.InformFinished(true)
				delta := int(util.CurrentTimeMs() - result.startTimeMs)
				md := util.MessageAndDuration{
					PublisherId:    worker.publisherId,
					SequenceNumber: result.sequenceNumber,
					LatencyMs:      delta,
				}
				worker.metricsTracker.Put(md)
			}
		} else {
			select {
			case <-worker.stopChannel:
				return
			case result := <-worker.resultChannel:
				results = append(results, result)
			}
		}
	}
}

func (worker *publisherWorker) loopingPublisher(request genproto.StartRequest) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, request.Project)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	options := request.GetPublisherOptions()

	topic := client.Topic(request.Topic)
	topic.PublishSettings.ByteThreshold = 9500000
	topic.PublishSettings.CountThreshold = int(options.BatchSize)
	delay, err := ptypes.Duration(options.BatchDuration)
	if err != nil {
		log.Fatalf("Given a bad publish batch duration: %v", err)
	}
	topic.PublishSettings.DelayThreshold = delay

	sequenceNumber := int32(0)
	data := make([]byte, options.MessageSize)
	for idx := range data {
		data[idx] = 'A'
	}

	for {
		select {
		case <-worker.stopChannel:
			worker.collectorStopChannel <- types.Nil{}
			return
		case count := <-worker.flowController.Start():
			for i := 0; i < count; i++ {
				publishTimeMs := util.CurrentTimeMs()
				result := topic.Publish(ctx, &pubsub.Message{
					Data: data,
					Attributes: map[string]string{
						"sendTime":       strconv.FormatInt(publishTimeMs, 10),
						"clientId":       strconv.FormatInt(worker.publisherId, 10),
						"sequenceNumber": strconv.FormatInt(int64(sequenceNumber), 10),
					},
				})
				worker.resultChannel <- augmentedPublishResult{
					result:         result,
					startTimeMs:    publishTimeMs,
					sequenceNumber: sequenceNumber,
				}
				sequenceNumber++
			}
		}
	}
}

func newPublisherWorker(
	request genproto.StartRequest,
	metricsTracker util.MetricsTracker,
	stopChannel <-chan types.Nil) *publisherWorker {
	options := request.GetPublisherOptions()
	var flowController flow_control.FlowController
	if options.Rate > 0 {
		flowController = flow_control.NewRateLimiterFlowController(options.Rate)
	} else {
		flowController = flow_control.NewOutstandingCountFlowController(
			kStartingPerWorkerBytesPerSecond / float64(options.MessageSize))
	}

	return &publisherWorker{
		publisherId:          rand.Int63(),
		metricsTracker:       metricsTracker,
		stopChannel:          stopChannel,
		flowController:       flowController,
		resultChannel:        make(chan augmentedPublishResult),
		collectorStopChannel: make(chan types.Nil),
	}
}

type publisherWorkerFactory struct{}

func (publisherWorkerFactory) runWorker(
	request genproto.StartRequest,
	metricsTracker util.MetricsTracker,
	stopChannel <-chan types.Nil) {
	worker := newPublisherWorker(request, metricsTracker, stopChannel)
	go worker.resultCollector()
	worker.loopingPublisher(request)
}

func (publisherWorkerFactory) numWorkers(request genproto.StartRequest) int {
	return util.ScaledNumWorkers(int(request.CpuScaling))
}

func NewPublisherTask() Task {
	return newTask(publisherWorkerFactory{})
}
