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
	"go/types"
	"google.com/cloud_pubsub_loadtest/internal/genproto"
	"google.com/cloud_pubsub_loadtest/internal/util"
	"log"
	"strconv"
)

const kBytesPerWorker = 100000000 // 100 MB per worker

const kOverallMessages = 10 * 1000 * 1000
// 10 million messages outstanding is necessary to prevent going OOM due to
// https://github.com/googleapis/google-cloud-go/issues/1318 on a 16 core, 60GB
// machine as numWorkers scales up.

type subscriberWorkerFactory struct{}

func (subscriberWorkerFactory) runWorker(
	request genproto.StartRequest,
	metricsTracker util.MetricsTracker,
	stopChannel <-chan types.Nil) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, request.Project)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	subscriber := client.Subscription(request.GetPubsubOptions().Subscription)
	subscriber.ReceiveSettings.MaxOutstandingMessages = kOverallMessages
	numWorkers := util.ScaledNumWorkers(int(request.CpuScaling))
	subscriber.ReceiveSettings.MaxOutstandingBytes = kBytesPerWorker * numWorkers
	subscriber.ReceiveSettings.NumGoroutines = numWorkers
	cctx, cancel := context.WithCancel(ctx)
	err = subscriber.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		sendTimeMs, err := strconv.ParseInt(msg.Attributes["sendTime"], 10, 64)
		if err != nil {
			panic(err)
		}
		clientId, err := strconv.ParseInt(msg.Attributes["clientId"], 10, 64)
		if err != nil {
			panic(err)
		}
		sequenceNumber, err := strconv.ParseInt(msg.Attributes["sequenceNumber"], 10, 32)
		if err != nil {
			panic(err)
		}

		delta := util.CurrentTimeMs() - sendTimeMs

		metricsTracker.Put(util.MessageAndDuration{
			PublisherId:    clientId,
			SequenceNumber: int32(sequenceNumber),
			LatencyMs:      int(delta),
		})
		msg.Ack()
	})
	if err != context.Canceled {
		log.Fatalf("Failed to create subscription: %v", err)
	}
	<-stopChannel
	log.Print("Cancelling subscriber")
	cancel()
}

func (subscriberWorkerFactory) numWorkers(request genproto.StartRequest) int { return 1 }

func NewSubscriberTask() Task {
	return newTask(subscriberWorkerFactory{})
}
