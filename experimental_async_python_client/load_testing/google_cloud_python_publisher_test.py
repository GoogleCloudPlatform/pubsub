import os
import time

from google.cloud import pubsub

from load_testing.element_counter import ElementCounter

done_counter: ElementCounter = ElementCounter()


def publish_done(future):
    message_id = future.result()
    global done_counter
    done_counter.increment()


if __name__ == "__main__":
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.expanduser("~/Downloads/creds.json")
    publisher = pubsub.PublisherClient()

    topic = "projects/pubsub-async-python/topics/test_topic"
    # 260 bytes is the average size of the messages on the pubsub-async-python topic
    to_publish = b"a" * 260
    start = time.time()
    while time.time() - start < 60:
        future = publisher.publish(topic, to_publish)
        future.add_done_callback(publish_done)

    delta_t = 60.0
    messages_sent = done_counter.read()
    bytes_per_second = (messages_sent * 260) / delta_t
    sent_per_second = messages_sent / delta_t

    print("bytes sent: {}, per second: {}".format(bytes_per_second, bytes_per_second))
    print("messages sent: {}, per second: {}".format(messages_sent, sent_per_second))
