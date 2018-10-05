import asyncio
import time
from multiprocessing import Value

from tornado.httpclient import AsyncHTTPClient

from fast_client.subscriber import MultiprocessSubscriber, BasicSubscriber
from google.oauth2 import service_account
import google.auth.transport.urllib3
import urllib3

from fast_client.types import PubsubMessage
from fast_client.types import SubscriptionInfo

global_message_counter: Value = None
global_byte_counter: Value = None


def byte_counter(message: PubsubMessage):
    global global_byte_counter
    with global_byte_counter.get_lock():
        global_byte_counter.value += len(message.data)


def message_counter(message: PubsubMessage):
    global global_message_counter
    with global_message_counter.get_lock():
        global_message_counter.value += 1


def counter(message: PubsubMessage):
    byte_counter(message)
    message_counter(message)
    return message.ack()


def noop(message):
    return message.ack()


async def counter_reader():
    global global_byte_counter
    global global_message_counter
    timestart = -1
    with global_byte_counter.get_lock():
        with global_message_counter.get_lock():
            timestart = time.time()
            global_byte_counter.value = 0
            global_message_counter.value = 0
    while True:
        await asyncio.sleep(5)
        bytes_received = -1
        messages_received = -1
        with global_byte_counter.get_lock():
            bytes_received = global_byte_counter.value
        with global_message_counter.get_lock():
            messages_received = global_message_counter.value

        delta_t = (time.time() - timestart) * 1.0
        bytes_per_second = bytes_received / delta_t
        messages_per_second = messages_received / delta_t

        print("bytes received: {}, bytes per second: {}".format(bytes_received, bytes_per_second))
        print("messages received: {}, messages per second: {}".format(messages_received, messages_per_second))


if __name__ == "__main__":
    global_byte_counter = Value('i', 0)
    global_message_counter = Value('i', 0)

    creds = service_account.Credentials.from_service_account_file("/Users/dpcollins/Downloads/subscriber_creds.json")

    scoped_credentials = creds.with_scopes(
        ['https://www.googleapis.com/auth/pubsub'])
    http = urllib3.PoolManager()
    request = google.auth.transport.urllib3.Request(http)
    scoped_credentials.refresh(request)
    print(scoped_credentials.token)

    info = SubscriptionInfo()
    info.project = "google.com:pubsub-sync-tail-test-1"
    info.subscription = "test_sub"
    info.token = scoped_credentials.token

    loop = asyncio.get_event_loop()
    #sub = BasicSubscriber(info, AsyncHTTPClient(), counter, loop)
    sub = MultiprocessSubscriber(info, counter)

    sub.start()
    asyncio.run_coroutine_threadsafe(counter_reader(), loop)
    loop.run_until_complete(asyncio.ensure_future(asyncio.sleep(100), loop=loop))
    sub.stop()
