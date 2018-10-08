import os
import time

from google.cloud import pubsub

from load_testing.element_counter import ElementCounter
from load_testing.message_counter import MessageCounter

message_counter: MessageCounter = MessageCounter()
done_counter: ElementCounter = ElementCounter()


def process_one(message):
    global message_counter
    message_counter.increment(message)
    message.ack()


if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.expanduser("~/Downloads/creds.json")
    subscriber = pubsub.SubscriberClient()

    subscription = "projects/pubsub-async-python/subscriptions/test_topic"
    future = subscriber.subscribe(subscription, process_one)
    time.sleep(60)
    future.cancel()

    delta_t = 60.0
    messages_received, bytes_received = message_counter.read()
    bytes_per_second = bytes_received / delta_t
    received_per_second = messages_received / delta_t

    print("bytes received: {}, per second: {}".format(bytes_received, bytes_per_second))
    print("messages received: {}, per second: {}".format(messages_received, received_per_second))
    print("average size: {}".format(bytes_received * 1.0 / messages_received))
