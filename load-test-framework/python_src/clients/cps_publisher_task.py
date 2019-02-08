from clients.task import Task, Worker, SubtaskWorker
from google.cloud.pubsub_v1.publisher import Client
from google.cloud.pubsub_v1.types import BatchSettings
from google.cloud.pubsub_v1.futures import Future
import random
import time
from clients.metrics_tracker import MessageAndDuration
import grpc
from concurrent import futures
from proto.loadtest_pb2 import StartRequest
from proto.loadtest_pb2_grpc import add_LoadtestWorkerServicer_to_server
from clients.loadtest_worker_servicer import LoadtestWorkerServicer
import sys
from clients.flow_control import FlowController, RateLimiterFlowController, OutstandingCountFlowController
from clients.to_float_seconds import to_float_seconds

# Start at 100 kB/s/worker
starting_per_worker_bytes_per_second = 10 ** 5


class PublisherSubtaskWorker(SubtaskWorker):
    def __init__(self):
        super().__init__()
        self._flow_controller: FlowController = None
        self._pub_id = int(random.random())

    def run_worker(self, request: StartRequest):
        message_size: int = request.publisher_options.message_size

        if request.publisher_options.rate > 0:
            self._flow_controller = RateLimiterFlowController(request.publisher_options.per_worker_rate)
        else:
            self._flow_controller = OutstandingCountFlowController(starting_per_worker_bytes_per_second / message_size)
        latency = to_float_seconds(request.publisher_options.batch_duration)
        client = Client(
            batch_settings=BatchSettings(
                max_latency=latency,
                max_bytes=9500000,
                max_messages=request.publisher_options.batch_size
            ))
        topic = client.topic_path(request.project, request.topic)
        data = b'A' * message_size
        sequence_number = 0

        while True:
            self._flow_controller.request_start()
            publish_time = int(time.time() * 1000)
            future: Future = client.publish(topic, data=data,
                                            sendTime=str(publish_time),
                                            clientId=str(self._pub_id),
                                            sequenceNumber=str(sequence_number))
            future.add_done_callback(lambda fut: self._on_publish(publish_time, sequence_number, fut))
            sequence_number = sequence_number + 1

    def _on_publish(self, publish_time: int, sequence_number: int, future: Future):
        try:
            future.result()
        except:
            self._flow_controller.inform_finished(False)
            return
        self._flow_controller.inform_finished(True)
        recv_time = int(time.time() * 1000)
        latency_ms = recv_time - publish_time
        out = MessageAndDuration(self._pub_id, sequence_number, latency_ms)
        self.metrics_tracker.put(out)


class CPSPublisherWorker(Worker):
    def __init__(self):
        super().__init__(PublisherSubtaskWorker())


class CPSPublisherTask(Task):
    @staticmethod
    def get_worker() -> Worker:
        return CPSPublisherWorker()


if __name__ == "__main__":
    port = "5000"
    for arg in sys.argv:
        if arg.startswith("--port="):
            port = arg.split("=")[1]
    Worker.print_flush('starting publisher')
    task = CPSPublisherTask()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    add_LoadtestWorkerServicer_to_server(LoadtestWorkerServicer(task), server)
    address = '[::]:' + port
    server.add_insecure_port(address)
    server.start()
    Worker.print_flush('publisher server started at ' + address)
    while True:
        time.sleep(1)
