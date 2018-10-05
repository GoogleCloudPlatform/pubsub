import typing
from urllib.parse import quote_plus


class TopicInfo:
    # The project name
    project: str
    # The unencoded topic name
    topic: str
    # The authorization token to be used
    token: str

    def url(self, method) -> str:
        return "https://pubsub.googleapis.com/v1/projects/{}/topics/{}:{}".format(
            self.project, quote_plus(self.topic), method)

    def header_map(self) -> typing.Dict[str, str]:
        return {
            "Authorization": "Bearer {}".format(self.token),
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
