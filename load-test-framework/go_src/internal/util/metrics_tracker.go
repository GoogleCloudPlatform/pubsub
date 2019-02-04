package util

import (
	"google.com/cloud_pubsub_loadtest/internal/genproto"
	"go/types"
	"math"
)

type MessageAndDuration struct {
	PublisherId int64
	SequenceNumber int32
	LatencyMs int
}

type MetricsTracker interface {
	Put(messageAndDuration MessageAndDuration)
	Check() genproto.CheckResponse
}

type metricsTrackerImpl struct {
	// Whether to track message ids
	trackIds bool
	// The current response value
	response genproto.CheckResponse
	// The channel to put values in
	putChannel chan MessageAndDuration
	// The channel for check requests
	checkRequestChannel chan types.Nil
	// The channel to send check responses to
	checkResponseChannel chan genproto.CheckResponse
}

func (mt *metricsTrackerImpl) Put(messageAndDuration MessageAndDuration) {
	mt.putChannel <- messageAndDuration
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

func NewMetricsTracker(trackIds bool) MetricsTracker {
	mt := &metricsTrackerImpl{
		trackIds: trackIds,
		putChannel: make(chan MessageAndDuration),
		checkRequestChannel: make(chan types.Nil),
		checkResponseChannel: make(chan genproto.CheckResponse),
	}
	// updater
	go func() {
		for {
			select {
			case messageAndDuration := <- mt.putChannel:
				bucket := bucketFor(messageAndDuration.LatencyMs)
				for len(mt.response.BucketValues) <= bucket {
					mt.response.BucketValues = append(mt.response.BucketValues, 0)
				}
				mt.response.BucketValues[bucket]++
				if trackIds {
					mt.response.ReceivedMessages = append(
						mt.response.ReceivedMessages, &genproto.MessageIdentifier{
							PublisherClientId: messageAndDuration.PublisherId,
							SequenceNumber: messageAndDuration.SequenceNumber,
						})
				}
			case <- mt.checkRequestChannel:
				mt.checkResponseChannel <- mt.response
				mt.response = genproto.CheckResponse{}
			}
		}
	}()
	return mt
}


