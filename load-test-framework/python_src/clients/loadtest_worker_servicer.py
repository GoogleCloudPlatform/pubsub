# Copyright 2019 Google LLC
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

from proto_dir.loadtest_pb2 import StartResponse
from proto_dir import loadtest_pb2_grpc
from clients.task import Task
from concurrent.futures import Executor, ThreadPoolExecutor
import time
from clients.to_float_seconds import to_float_seconds


class LoadtestWorkerServicer(loadtest_pb2_grpc.LoadtestWorkerServicer):
    """Provides methods that implement functionality of load test server."""

    def __init__(self, task):
        self.task = task  # type: Task
        self.executor: Executor = ThreadPoolExecutor(max_workers=1)
        self.stopped = False
        self.start_time = None
        self.test_duration = None

    def Start(self, request, context):
        self.task.start(request)
        self.start_time = to_float_seconds(request.start_time)
        self.test_duration = to_float_seconds(request.test_duration)
        self.executor.submit(self.wait_then_stop)
        return StartResponse()

    def _time_since_start(self):
        return time.time() - self.start_time

    def wait_then_stop(self):
        time.sleep((self.start_time + self.test_duration) - time.time())
        self.task.stop()
        self.stopped = True

    def Check(self, request, context):
        response = self.task.check()
        response.running_duration.seconds = int(self._time_since_start())
        response.is_finished = self.stopped
        return response
