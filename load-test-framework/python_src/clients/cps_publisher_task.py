#!/usr/bin/env python

# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time

import grpc
from concurrent import futures
from google.cloud import pubsub

import loadtest_pb2


class LoadtestServicer(loadtest_pb2.LoadtestServicer):
    """Provides methods that implement functionality of load test server."""

    def __init__(self):
        self.pubsub_client = pubsub.Client()
        self.message_size = None
        self.batch_size = None
        self.batch = None

    def Start(self, request, context):
        self.message_size = request.message_size
        self.batch_size = request.pubsub_options.publish_batch_size
        self.batch = self.pubsub_client.topic(request.topic).batch()
        return loadtest_pb2.StartResponse()

    def Check(self, request, context):
        if not self.batch:
            return loadtest_pb2.CheckResponse()
        start = time.clock()
        for i in range(0, request.pubsub_options.publish_batch_size):
            self.batch.publish("A" * request.message_size, sendTime=str(int(start * 1000)))
        end = time.clock()
        response = loadtest_pb2.CheckResponse()
        response.bucket_values = [(end - start) * 1000] * self.batch_size
        return response


if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    loadtest_pb2.add_LoadtestServicer_to_server(LoadtestServicer(), server)
    server.add_insecure_port('localhost:6000')
    server.start()
    while True:
        time.sleep(3600)
