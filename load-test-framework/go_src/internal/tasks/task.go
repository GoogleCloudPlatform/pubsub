package tasks

import (
	"github.com/golang/protobuf/ptypes"
	"go/types"
	"google.com/cloud_pubsub_loadtest/internal/genproto"
	"google.com/cloud_pubsub_loadtest/internal/util"
	"log"
	"runtime"
	"time"
)

type Task interface {
	Start(request genproto.StartRequest)
	Stop()
	Check() genproto.CheckResponse
}

type subtaskWorkerFactory interface {
	// Returns the number of workers to start
	numWorkers(request genproto.StartRequest) int
	// Starts a worker that will stop when it receives a message on stopChannel
	runWorker(
		request genproto.StartRequest,
		metricsTracker util.MetricsTracker,
		stopChannel <-chan types.Nil)
}

type baseTask struct {
	metricsTracker     util.MetricsTracker
	workerFactory      subtaskWorkerFactory
	workerStopChannels []chan<- types.Nil
	startTime          time.Time
	stopped            bool
}

func (task *baseTask) Start(request genproto.StartRequest) {
	task.metricsTracker = util.NewMetricsTracker(request.IncludeIds);
	if pubOptions := request.GetPublisherOptions(); pubOptions != nil {
		pubOptions.Rate /= float32(runtime.NumCPU())
	}
	startTime, err := ptypes.Timestamp(request.StartTime)
	if err != nil {
		log.Fatalf("Failed to parse start time: %v", err)
	}
	task.startTime = startTime
	runDuration, err := ptypes.Duration(request.TestDuration)
	if err != nil {
		log.Fatalf("Failed to parse test duration: %v", err)
	}
	endTime := startTime.Add(runDuration)
	go func() {
		time.Sleep(endTime.Sub(time.Now()))
		task.stopped = true
		task.Stop()
	}()
	go func() {
		time.Sleep(startTime.Sub(time.Now()))
		for i := 0; i < task.workerFactory.numWorkers(request); i++ {
			stopChan := make(chan types.Nil)
			task.workerStopChannels = append(task.workerStopChannels, stopChan)
			go task.workerFactory.runWorker(
				request, task.metricsTracker, stopChan)
		}
	}()

	return
}

func (task *baseTask) Stop() {
	for _, stopChan := range task.workerStopChannels {
		stopChan <- types.Nil{}
	}
}

func (task *baseTask) Check() genproto.CheckResponse {
	response := task.metricsTracker.Check()
	response.RunningDuration = ptypes.DurationProto(time.Now().Sub(task.startTime))
	response.IsFinished = task.stopped
	return response
}

func newTask(workerFactory subtaskWorkerFactory) Task {
	return &baseTask{
		workerFactory: workerFactory,
	}
}
