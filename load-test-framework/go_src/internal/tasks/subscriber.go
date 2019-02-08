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
	subscriber.ReceiveSettings.MaxOutstandingMessages = 1e6
	subscriber.ReceiveSettings.NumGoroutines = util.ScaledNumWorkers(int(request.CpuScaling))
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
	if err != nil {
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
