import base64
import json
from json import JSONEncoder
from typing import Dict, AsyncIterator, Optional
from datetime import datetime


class UserPubsubMessage:
    data: bytes  # raw message data
    attributes: Dict[str, str]  # attributes

    def __init__(self, data: bytes, attributes: Dict[str, str]):
        self.data = data
        self.attributes = attributes


class UserPubsubMessageEncoder(JSONEncoder):
    def default(self, o: UserPubsubMessage):
        if isinstance(o, UserPubsubMessage):
            return {
                "data": base64.b64encode(o.data).decode("utf-8"),
                "attributes": o.attributes
            }
        else:
            return json.JSONEncoder.default(self, o)


class PubsubMessage:
    data: bytes                 # raw message data
    attributes: Dict[str, str]  # attributes
    messageId: str              # message id
    publishTime: datetime       # publish time
    _ackId: Optional[str]       # ackId for use with ack()

    def __init__(self, data: bytes, attributes: Dict[str, str], messageId: str, publishTime: datetime,
                 ackId: Optional[str] = None):
        self.data = data
        self.attributes = attributes
        self.messageId = messageId
        self.publishTime = publishTime
        self._ackId = ackId

    def ack(self):
        assert self._ackId
        return self._ackId
