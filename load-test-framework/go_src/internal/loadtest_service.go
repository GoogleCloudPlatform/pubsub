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
