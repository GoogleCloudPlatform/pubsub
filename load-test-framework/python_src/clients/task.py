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

import time
from abc import ABC, abstractmethod
from concurrent.futures import Executor
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Lock
from typing import Optional

import grpc

from proto_dir.loadtest_pb2 import StartRequest, StartResponse, CheckRequest, CheckResponse
from multiprocessing import Queue, Process, cpu_count
from clients.metrics_tracker import MetricsTracker, combine_responses
import sys

from proto_dir.loadtest_pb2_grpc import LoadtestWorkerServicer, add_LoadtestWorkerServicer_to_server, LoadtestWorkerStub


class SubtaskWorker(LoadtestWorkerServicer):
    """
    SubtaskWorker must be pickleable before Start is called.
    """

    def __init__(self):
        self._executor: Executor = None
        self.metrics_tracker: MetricsTracker = None

    @abstractmethod
    def run_worker(self, request: StartRequest):
        """
        run_worker should run the worker forever.
        :param request: the request to start the worker
        :return: never
        """
        pass

    def Start(self, request: StartRequest, context):
        self._executor = ThreadPoolExecutor(max_workers=1)
        self.metrics_tracker = MetricsTracker(request.include_ids)
        self._executor.submit(self.run_worker, request)
        return StartResponse()

    def Check(self, request, context):
        return self.metrics_tracker.check()


class Worker(ABC):
    """
    Processes must be forked before GRPC server starts due to a GRPC bug.
    """

    def __init__(self, worker: SubtaskWorker):
        port_queue = Queue()
        self._process = Process(target=self.run_worker, args=(port_queue, worker))
        self._process.start()
        self._port = port_queue.get()

        self._stub_lock: Lock = None
        self._stub: Optional[LoadtestWorkerStub] = None

    @staticmethod
    def run_worker(port_queue: "Queue[int]", worker: SubtaskWorker):
        server = grpc.server(ThreadPoolExecutor(max_workers=1))
        add_LoadtestWorkerServicer_to_server(worker, server)
        address = "localhost:0"
        port: int = server.add_insecure_port(address)
        server.start()
        Worker.print_flush("started subtask at port " + str(port))
        port_queue.put(port)
        while True:
            time.sleep(1)

    @staticmethod
    def print_flush(to_print):
        print(to_print)
        sys.stdout.flush()

    def start(self, request: StartRequest):
        channel = grpc.insecure_channel("localhost:" + str(self._port))
        self._stub_lock = Lock()
        self._stub = LoadtestWorkerStub(channel)
        self.print_flush("Notifying worker of start")
        self._stub.Start(request)

    def check(self) -> CheckResponse:
        with self._stub_lock:
            if self._stub is not None:
                return self._stub.Check(CheckRequest())
        return CheckResponse()

    def stop(self):
        with self._stub_lock:
            self._stub = None
        self._process.terminate()


class Task(ABC):
    def __init__(self):
        self.workers_ = [self.get_worker() for _ in range(cpu_count())]

    @staticmethod
    @abstractmethod
    def get_worker() -> Worker:
        pass

    def start(self, request: StartRequest):
        if request.HasField("publisher_options"):
            request.publisher_options.rate /= len(self.workers_)
        for worker in self.workers_:
            worker.start(request)

    def stop(self):
        for worker in self.workers_:
            worker.stop()

    def check(self):
        responses = [worker.check() for worker in self.workers_]
        return combine_responses(responses)
