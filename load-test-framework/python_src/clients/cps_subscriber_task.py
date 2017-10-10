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

import sys
import threading
import time

from concurrent import futures
import grpc
import loadtest_pb2
from google.cloud.pubsub_v1 import subscriber

class LoadtestWorkerServicer(loadtest_pb2.LoadtestWorkerServicer):
    """Provides methods that implement functionality of load test server."""

    def __init__(self):
        self.lock = threading.Lock()
        self.latencies = []
        self.recv_msgs = []

    def ProcessMessage(self, message):
        latency = int(time.time() * 1000 - int(message.attributes()["sendTime"]))
        identifier = loadtest_pb2.MessageIdentifier()
        identifier.publisher_client_id = int(message.attributes()["clientId"])
        identifier.sequence_number = int(message.attributes()["sequenceNumber"])
        message.ack()
        self.lock.acquire()
        self.latencies.append(latency)
        self.recv_msgs.append(identifier)
        self.lock.release()

    def Start(self, request, context):
        self.message_size = request.message_size
        self.batch_size = request.publish_batch_size
        subscription = "projects/" + request.project + "/subscriptions/" + request.pubsub_options.subscription
        self.client = subscriber.Client()
        self.client.subscribe(subscription, lambda msg: self.ProcessMessage(msg))
        return loadtest_pb2.StartResponse()

    def Execute(self, request, context):
        response = loadtest_pb2.ExecuteResponse()
        self.lock.acquire()
        response.latencies.extend(self.latencies)
        response.received_messages.extend(self.recv_msgs)
        self.latencies = []
        self.recv_msgs = []
        self.lock.release()
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
