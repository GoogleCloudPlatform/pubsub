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

package flow_control

import (
	"math"
	"sync"
	"time"
)

type cyclicBucketer struct {
	mu sync.Mutex
	// The per-second counts for this bucketer
	buckets []int
	// The current bucket this is on
	bucketIndex int
	// Whether this has cycled
	hasCycled bool
}

func newCyclicBucketer(size int) *cyclicBucketer {
	return &cyclicBucketer{
		buckets: make([]int, size),
	}
}

// cycle the cyclicBucketer, returning the current sum of all buckets and whether the
// bucketer has cycled, and zeroing out the next one.
func (cb *cyclicBucketer) cycle() (sum int, hasCycled bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	sum = 0
	for _, value := range cb.buckets {
		sum += value
	}
	cb.bucketIndex = (cb.bucketIndex + 1) % len(cb.buckets)
	cb.hasCycled = cb.hasCycled || (cb.bucketIndex == 0)
	cb.buckets[cb.bucketIndex] = 0
	return sum, cb.hasCycled
}

// add to the current cyclicBucketer bucket
func (cb *cyclicBucketer) add() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.buckets[cb.bucketIndex] += 1
}

// OutstandingCountFlowController is intended to be used from a single
// goroutine.
type outstandingCountFlowController struct {
	// The channel to wait on to start
	incrementChan chan int
	// The channel to submit completion to
	decrementChan chan bool

	// The channel to update the rate per second
	updateChan chan float64
	// The current estimated publish rate per second
	ratePerSecond float64
	// The current outstanding entries
	outstanding int64
	// The bucketer controlling the rate change
	bucketer *cyclicBucketer
}

func (fc *outstandingCountFlowController) updateRate(newRate float64) {
	fc.updateChan <- newRate
}

func (fc *outstandingCountFlowController) Start() <-chan int {
	return fc.incrementChan
}

func (fc *outstandingCountFlowController) InformFinished(wasSuccessful bool) {
	fc.decrementChan <- wasSuccessful
}

func (fc *outstandingCountFlowController) numAllowedPermits() int {
	return int((fc.ratePerSecond * 2) - float64(fc.outstanding))
}

const expiryLatencyMilliseconds = 15000
const rateUpdateDelayMilliseconds = 100

// The size of one 10MB batch of 1KB messages
const finishedBufferSize = 10000

func NewOutstandingCountFlowController(initialRate float64) FlowController {
	fc := &outstandingCountFlowController{
		incrementChan: make(chan int),
		decrementChan: make(chan bool, finishedBufferSize),
		updateChan:    make(chan float64),
		ratePerSecond: math.Max(initialRate, 1),
		bucketer:      newCyclicBucketer(expiryLatencyMilliseconds / rateUpdateDelayMilliseconds),
	}
	// Latency updater
	go func() {
		for {
			time.Sleep(time.Millisecond * rateUpdateDelayMilliseconds)
			sum, hasCycled := fc.bucketer.cycle()
			if hasCycled {
				fc.updateRate(float64(sum) / (expiryLatencyMilliseconds / 1000))
			}
		}
	}()
	// rate handler
	go func() {
		for {
			allowedPermits := fc.numAllowedPermits()
			if allowedPermits > 0 {
				select {
				case fc.incrementChan <- allowedPermits:
					fc.outstanding += int64(allowedPermits)
				case wasSuccessful := <-fc.decrementChan:
					fc.outstanding--
					if wasSuccessful {
						fc.bucketer.add()
					}
				case newRate := <-fc.updateChan:
					fc.ratePerSecond = newRate
				}
			} else {
				select {
				case wasSuccessful := <-fc.decrementChan:
					fc.outstanding--
					if wasSuccessful {
						fc.bucketer.add()
					}
				case newRate := <-fc.updateChan:
					fc.ratePerSecond = newRate
				}
			}
		}
	}()
	return fc
}
