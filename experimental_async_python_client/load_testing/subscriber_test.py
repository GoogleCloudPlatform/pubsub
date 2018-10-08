import asyncio
import os
import time

from tornado.httpclient import AsyncHTTPClient

from fast_client.subscriber import MultiprocessSubscriber, BasicSubscriber, Subscriber
from google.oauth2 import service_account
import google.auth.transport.urllib3
import urllib3

from fast_client.core import SubscriptionInfo, PubsubMessage
from load_testing.element_counter import ElementCounter
from load_testing.message_counter import MessageCounter

message_counter: MessageCounter = MessageCounter()
done_counter: ElementCounter = ElementCounter()


async def counter_reader():
    global message_counter
    global done_counter
    time_start = time.time()
    while True:
        await asyncio.sleep(5)
        messages_received, bytes_received = message_counter.read()
        messages_acked = done_counter.read()

        delta_t = (time.time() - time_start) * 1.0
        bytes_per_second = bytes_received / delta_t
        received_per_second = messages_received / delta_t
        acked_per_second = messages_acked / delta_t

        print("bytes received: {}, per second: {}".format(bytes_received, bytes_per_second))
        print("messages received: {}, per second: {}".format(messages_received, received_per_second))
        print("average size: {}".format(bytes_received * 1.0 / messages_received))
        print("messages acked: {}, per second: {}".format(messages_acked, acked_per_second))


def process_one(message: PubsubMessage):
    global message_counter
    message_counter.increment(message)
    return message.ack()


def one_ack_done(ackId: str):
    global done_counter
    done_counter.increment()


async def run_sub_for(subscriber: Subscriber, min_run_seconds: float):
    await asyncio.sleep(min_run_seconds)
    subscriber.stop()
    while subscriber.is_running():
        await asyncio.sleep(.5)


if __name__ == "__main__":
    creds = service_account.Credentials.from_service_account_file(os.path.expanduser("~/Downloads/creds.json"))

    scoped_credentials = creds.with_scopes(
        ['https://www.googleapis.com/auth/pubsub'])
    http = urllib3.PoolManager()
    request = google.auth.transport.urllib3.Request(http)
    scoped_credentials.refresh(request)
    print(scoped_credentials.token)

    info = SubscriptionInfo()
    info.project = "pubsub-async-python"
    info.subscription = "test_sub"
    info.token = scoped_credentials.token

    loop = asyncio.get_event_loop()
    # sub = BasicSubscriber(info, AsyncHTTPClient(), process_one, one_ack_done, loop)
    sub = MultiprocessSubscriber(info, process_one, one_ack_done)

    sub.start()
    asyncio.run_coroutine_threadsafe(counter_reader(), loop)
    loop.run_until_complete(run_sub_for(sub, 60))
