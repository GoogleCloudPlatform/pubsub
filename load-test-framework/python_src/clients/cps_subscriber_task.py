from clients.task import Task, Worker, SubtaskWorker
from google.cloud.pubsub_v1.subscriber import Client
from google.cloud.pubsub_v1.subscriber.message import Message
import time
from clients.metrics_tracker import MessageAndDuration
import grpc
from concurrent import futures

from proto.loadtest_pb2 import StartRequest
from proto.loadtest_pb2_grpc import add_LoadtestWorkerServicer_to_server
from clients.loadtest_worker_servicer import LoadtestWorkerServicer
import sys


class SubscriberSubtaskWorker(SubtaskWorker):
    def run_worker(self, request: StartRequest):
        Worker.print_flush("started subscriber")
        subscription = "projects/" + request.project + "/subscriptions/" + request.pubsub_options.subscription
        client = Client()
        client.subscribe(subscription, self._on_receive).result()

    def _on_receive(self, message: Message):
        recv_time = int(time.time() * 1000)
        latency_ms = recv_time - int(message.attributes["sendTime"])
        pub_id = int(message.attributes["clientId"])
        sequence_number = int(message.attributes["sequenceNumber"])
        out = MessageAndDuration(pub_id, sequence_number, latency_ms)
        self.metrics_tracker.put(out)
        message.ack()


class CPSSubscriberWorker(Worker):
    def __init__(self):
        super().__init__(SubscriberSubtaskWorker())


class CPSSubscriberTask(Task):
    @staticmethod
    def get_worker() -> Worker:
        return CPSSubscriberWorker()


if __name__ == "__main__":
    port = "5000"
    for arg in sys.argv:
        if arg.startswith("--port="):
            port = arg.split("=")[1]
    task = CPSSubscriberTask()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    add_LoadtestWorkerServicer_to_server(LoadtestWorkerServicer(task), server)
    address = '[::]:' + port
    server.add_insecure_port(address)
    server.start()
    print('subscriber server started at ' + address)
    while True:
        time.sleep(1)
