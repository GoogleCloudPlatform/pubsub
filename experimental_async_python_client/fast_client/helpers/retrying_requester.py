from typing import Awaitable

from tornado.httpclient import HTTPRequest, HTTPResponse, AsyncHTTPClient, HTTPError


async def retrying_requester(request: HTTPRequest, client: AsyncHTTPClient) -> Awaitable[HTTPResponse]:
    done = False

    while not done:
        try:
            result: HTTPResponse = await client.fetch(request)
            done = True
            return result
        except HTTPError as e:
            print(e)
            continue
