from typing import Awaitable

from tornado.httpclient import HTTPRequest, HTTPResponse, AsyncHTTPClient, HTTPError


async def retrying_requester(request: HTTPRequest, client: AsyncHTTPClient) -> Awaitable[HTTPResponse]:
    while True:
        result: HTTPResponse = await client.fetch(request, raise_error=False)
        if result.code == 200:
            return result
        else:
            print("url: {}, headers: {}, error: {}".format(request.url, request.headers, result.error))
            print(result.body)
