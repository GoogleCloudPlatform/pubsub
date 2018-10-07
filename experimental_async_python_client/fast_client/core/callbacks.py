from typing import Callable, Optional, Awaitable, Union

from fast_client.core import PubsubMessage, UserPubsubMessage

SubscriberSyncT = Callable[[PubsubMessage], Optional[str]]
SubscriberAsyncT = Callable[[PubsubMessage], Awaitable[Optional[str]]]
SubscriberActionT = Union[SubscriberSyncT, SubscriberAsyncT]

PublisherSyncT = Callable[[], UserPubsubMessage]
PublisherAsyncT = Callable[[], Awaitable[UserPubsubMessage]]
PublisherActionT = Union[PublisherSyncT, PublisherAsyncT]
