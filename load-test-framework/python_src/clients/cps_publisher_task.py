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

import random
import sys
import threading
import time

from concurrent import futures
import grpc
import loadtest_pb2
from google.cloud.pubsub_v1 import publisher
from google.cloud.pubsub_v1.types import BatchSettings

class LoadtestWorkerServicer(loadtest_pb2.LoadtestWorkerServicer):
    """Provides methods that implement functionality of load test server."""

    def __init__(self):
        self.lock = threading.Lock()
        self.message_size = None
        self.batch_size = None
        self.batch = None
        self.num_msgs_published = 0
        self.id = str(int(random.random() * (2 ** 32 - 1)))
        self.latencies = []

    def Start(self, request, context):
        self.message_size = request.message_size
        self.batch_size = request.publish_batch_size
        self.topic = "projects/" + request.project + "/topics/" + request.topic
        self.client = publisher.Client(batch_settings=BatchSettings(max_latency=float(request.publish_batch_duration.seconds) + float(request.publish_batch_duration.nanos) / 1000000000.0))
        return loadtest_pb2.StartResponse()

    def OnPublishDone(self, start, future):
        if future.exception() is not None:
            return
        end = time.time()
        self.lock.acquire()
        self.latencies.append(int((end - start) * 1000))
        self.lock.release()

    def Execute(self, request, context):
        self.lock.acquire()
        sequence_number = self.num_msgs_published
        self.num_msgs_published += self.batch_size
        latencies = self.latencies
        self.latencies = []
        self.lock.release()
        start = time.time()
        for i in range(0, self.batch_size):
            self.client.publish(self.topic, ("A" * self.message_size).encode(),
                                sendTime=str(int(time.time() * 1000)),
                                clientId=self.id,
                                sequenceNumber=str(int(sequence_number + i))).add_done_callback(lambda f: self.OnPublishDone(start, f))
        response = loadtest_pb2.ExecuteResponse()
        response.latencies.extend(latencies)
        return response


if __name__ == "__main__":
    port = "6000"
    for arg in sys.argv:
      if arg.startswith("--worker_port="):
        port = arg.split("=")[1]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1000))
    loadtest_pb2.add_LoadtestWorkerServicer_to_server(LoadtestWorkerServicer(), server)
    server.add_insecure_port('localhost:' + port)
    server.start()
    while True:
        time.sleep(3600)
