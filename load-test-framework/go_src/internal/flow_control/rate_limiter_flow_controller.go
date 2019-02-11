package flow_control

import (
	"time"
)

type rateLimiterFlowController struct {
	startChan <-chan int
}

func (fc *rateLimiterFlowController) Start() <-chan int {
	return fc.startChan
}

func (fc *rateLimiterFlowController) InformFinished(wasSuccessful bool) {}

func NewRateLimiterFlowController(requestsPerSecond float32) FlowController {
	secondsPerRequest := 1 / requestsPerSecond
	ticker := time.NewTicker(time.Duration(secondsPerRequest) * time.Second)
	startChan := make(chan int)
	go func() {
		for range ticker.C {
			startChan <- 1
		}
	}()
	return &rateLimiterFlowController{
		startChan: startChan,
	}
}
