import asyncio
import os
import time
from functools import partial

from tornado.httpclient import AsyncHTTPClient

from google.oauth2 import service_account
import google.auth.transport.urllib3
import urllib3

from fast_client.publisher import BasicPublisher, Publisher, MultiprocessPublisher, MultiprocessPublisherOptions
from fast_client.core import TopicInfo, UserPubsubMessage
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
        messages_sent, bytes_sent = message_counter.read()
        messages_completed = done_counter.read()

        delta_t = (time.time() - time_start) * 1.0
        bytes_per_second = bytes_sent / delta_t
        sent_per_second = messages_sent / delta_t
        completed_per_second = messages_completed / delta_t

        print("bytes sent: {}, per second: {}".format(bytes_sent, bytes_per_second))
        print("messages sent: {}, per second: {}".format(messages_sent, sent_per_second))
        print("messages completed: {}, per second: {}".format(messages_completed, completed_per_second))


def publish(message: UserPubsubMessage):
    global message_counter
    message_counter.increment(message)
    return message


async def run_pub_for(publisher: Publisher, min_run_seconds: float):
    await asyncio.sleep(min_run_seconds)
    publisher.stop()
    while publisher.is_running():
        await asyncio.sleep(.5)


def publisher_done(_: str):
    global done_counter
    done_counter.increment()


if __name__ == "__main__":
    creds = service_account.Credentials.from_service_account_file(os.path.expanduser("~/Downloads/creds.json"))

    scoped_credentials = creds.with_scopes(
        ['https://www.googleapis.com/auth/pubsub'])
    http = urllib3.PoolManager()
    request = google.auth.transport.urllib3.Request(http)
    scoped_credentials.refresh(request)
    print(scoped_credentials.token)

    info = TopicInfo()
    info.project = "pubsub-async-python"
    info.topic = "test_topic"
    info.token = scoped_credentials.token

    # 260 bytes is the average size of the messages on the pubsub-async-python topic
    to_publish = UserPubsubMessage(b"a"*260, {"a": "b", "c": "d"})

    loop = asyncio.get_event_loop()
    # pub = BasicPublisher(info, AsyncHTTPClient(), partial(publish, to_publish), publisher_done, loop)
    options = MultiprocessPublisherOptions()
    options.publisher_options.writer_options.write_batch_size_bytes = 10**5
    pub = MultiprocessPublisher(info, partial(publish, to_publish), publisher_done, options)

    pub.start()
    asyncio.run_coroutine_threadsafe(counter_reader(), loop)
    loop.run_until_complete(run_pub_for(pub, 60))
