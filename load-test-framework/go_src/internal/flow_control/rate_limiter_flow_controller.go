package flow_control

import (
	"go/types"
	"time"
)

type rateLimiterFlowController struct {
	startChan <-chan types.Nil
}

func (fc *rateLimiterFlowController) Start() <-chan types.Nil {
	return fc.startChan
}

func (fc *rateLimiterFlowController) InformFinished(wasSuccessful bool) {}

func NewRateLimiterFlowController(requestsPerSecond float32) FlowController {
	secondsPerRequest := 1 / requestsPerSecond
	ticker := time.NewTicker(time.Duration(secondsPerRequest) * time.Second)
	startChan := make(chan types.Nil)
	go func() {
		for range ticker.C {
			startChan <- types.Nil{}
		}
	}()
	return &rateLimiterFlowController{
		startChan: startChan,
	}
}
