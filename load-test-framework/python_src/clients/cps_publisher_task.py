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

import grpc
import sys
import time

from concurrent import futures
from google.cloud import pubsub

import loadtest_pb2


class LoadtestWorkerServicer(loadtest_pb2.LoadtestWorkerServicer):
    """Provides methods that implement functionality of load test server."""

    def __init__(self):
        self.message_size = None
        self.batch_size = None
        self.batch = None

    def Start(self, request, context):
        self.message_size = request.message_size
        self.batch_size = request.pubsub_options.publish_batch_size
        self.batch = pubsub.Client().topic(request.topic).batch()
        return loadtest_pb2.StartResponse()

    def Execute(self, request, context):
        start = time.clock()
        for i in range(0, self.batch_size):
            self.batch.publish(("A" * self.message_size).encode(), sendTime=str(int(start * 1000)))
        self.batch.commit()
        end = time.clock()
        response = loadtest_pb2.ExecuteResponse()
        response.latencies.extend([int((end - start) * 1000)] * self.batch_size)
        return response


if __name__ == "__main__":
    port = 6000
    for arg in sys.argv:
      if arg.startswith("--worker_port="):
        port = arg.split("=")[1]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    loadtest_pb2.add_LoadtestWorkerServicer_to_server(LoadtestWorkerServicer(), server)
    server.add_insecure_port('localhost:' + port)
    server.start()
    while True:
        time.sleep(3600)
