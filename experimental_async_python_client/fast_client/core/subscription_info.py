import typing
from urllib.parse import quote_plus


class SubscriptionInfo:
    # The project name
    project: str
    # The unencoded subscription name
    subscription: str
    # The authorization token to be used
    token: str

    def url(self, method) -> str:
        return "https://pubsub.googleapis.com/v1/projects/{}/subscriptions/{}:{}".format(
            self.project, quote_plus(self.subscription), method)

    def header_map(self) -> typing.Dict[str, str]:
        return {
            "Authorization": "Bearer {}".format(self.token),
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
