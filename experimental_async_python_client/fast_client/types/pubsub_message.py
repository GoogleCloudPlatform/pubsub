from typing import Dict, AsyncIterator, Optional
from datetime import datetime


class UserPubsubMessage:
    data: bytes  # raw message data
    attributes: Dict[str, str]  # attributes

    def __init__(self, data: bytes, attributes: Dict[str, str]):
        self.data = data
        self.attributes = attributes


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
