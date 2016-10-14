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


import argparse
import socket
import time

from google.cloud import pubsub

SERVER_ADDRESS = "./client_socket"

if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('topic')
    parser.add_argument('message_size')
    args = parser.parse_args()

    message_size = int(args.message_size)

    # Create Pub/Sub client
    pubsub_client = pubsub.Client()
    topic = pubsub_client.topic(args.topic)

    # Bind to local socket
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(SERVER_ADDRESS)
    sock.listen(1)

    # Continously Publish
    while True:
        connection, client_address = sock.accept()
        while True:
            try:
                connection.recv(1)  # Server will send 1 byte every time we want to publish
                start = time.clock()
                topic.publish("A" * message_size, sendTime=str(int(start * 1000)))
                end = time.clock()
                connection.sendall(int((start - end) * 1000))
            finally:
                connection.close()
