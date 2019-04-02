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

import math
from threading import Lock
from typing import List
import sys

from proto.loadtest_pb2 import CheckResponse


class MessageAndDuration:
    def __init__(self, publisher_id: int, sequence_number: int, latency_ms: int):
        """
        Initialize a MessageAndDuration
        :param publisher_id: the integer publisher id
        :param sequence_number: the integer sequence number for this publisher
        :param latency_ms: the latency for this message
        """
        self.publisher_id = publisher_id
        self.sequence_number = sequence_number
        self.latency_ms = latency_ms


class MetricsTracker:
    def __init__(self, include_ids: bool):
        self.lock_ = Lock()
        self.to_fill = CheckResponse()
        self.to_fill.failed = 0
        self.include_ids = include_ids

    @staticmethod
    def _bucket_for(latency_ms: int):
        raw_bucket = int(math.floor(math.log(latency_ms + sys.float_info.min, 1.5)))
        return max(0, raw_bucket)

    def put(self, value: MessageAndDuration):
        bucket = self._bucket_for(value.latency_ms)

        with self.lock_:
            while len(self.to_fill.bucket_values) <= bucket:
                self.to_fill.bucket_values.append(0)
            self.to_fill.bucket_values[bucket] += 1
            if self.include_ids:
                message_id = self.to_fill.received_messages.add()
                message_id.publisher_client_id = value.publisher_id
                message_id.sequence_number = value.sequence_number

    def put_error(self):
        with self.lock_:
            self.to_fill.failed += 1

    def check(self):
        with self.lock_:
            out = self.to_fill
            self.to_fill = CheckResponse()
            self.to_fill.failed = 0
            return out


def combine_responses(responses: List[CheckResponse]):
    out = CheckResponse()
    for response in responses:
        while len(out.bucket_values) < len(response.bucket_values):
            out.bucket_values.append(0)
        for idx, value in enumerate(response.bucket_values):
            out.bucket_values[idx] += value
        out.received_messages.extend(response.received_messages)
    return out
