package internal

import (
	"context"
	"google.com/cloud_pubsub_loadtest/internal/genproto"
	"google.com/cloud_pubsub_loadtest/internal/tasks"
)

type loadtestWorkerService struct {
	workerTask tasks.Task
}

func (service *loadtestWorkerService) Check(context.Context, *genproto.CheckRequest) (*genproto.CheckResponse, error) {
	response := service.workerTask.Check()
	return &response, nil
}

func (service *loadtestWorkerService) Start(
	ctx context.Context, request *genproto.StartRequest) (*genproto.StartResponse, error) {
	service.workerTask.Start(*request)
	return &genproto.StartResponse{}, nil
}

func NewLoadtestWorkerService(isPublisher bool) genproto.LoadtestWorkerServer {
	if isPublisher {
		return &loadtestWorkerService{
			workerTask: tasks.NewPublisherTask(),
		}
	} else {
		return &loadtestWorkerService{
			workerTask: tasks.NewSubscriberTask(),
		}
	}
}
