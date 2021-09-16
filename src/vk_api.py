import re

import aiohttp as aiohttp
import requests as requests
import urllib.parse as urlparse

VK_API_URL = "https://api.vk.com"
GROUP_MEMBERS_METHOD = 'groups.getMembers'
USERS_GET_METHOD = 'users.get'


class VkAPI:
    access_token: str
    v: str

    def __init__(self, access_token, api_version):
        self.access_token = access_token
        self.v = api_version

    def __generate_params(self, **params) -> str:
        p = {'access_token': self.access_token, 'v': self.v}
        p.update(params)

        return urlparse.urlencode(p)

    def __generate_uri(self, method: str, **params) -> str:
        params = self.__generate_params(**params)

        return f'{VK_API_URL}/method/{method}?{params}'

    @staticmethod
    async def __request(uri: str, method='GET', body=None, headers=None) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.request(method, uri, json=body, headers=headers) as response:
                try:
                    return await response.json()
                except Exception as e:
                    print(e)
                    return {}

    async def request(self, api_method: str, params: dict, method: str = 'GET', body=None, headers=None):
        uri = self.__generate_uri(api_method, **params)
        print(uri)
        response = await self.__request(uri, method, body, headers)

        return response

    async def get_group_members(self, group_id: str, offset: int) -> list:
        response = await self.request(GROUP_MEMBERS_METHOD, {'group_id': group_id, 'offset': offset})

        print(response)

        return response

    async def get_users(self, user_ids: str) -> dict:
        response = await self.request(USERS_GET_METHOD, {'user_ids': user_ids})

        print(response)

        return response

    async def test(self):
        res = await self.get_group_members('mzk', 1000)
        items = res['response']['items']
        print(len(items))
        items = items[:1000]
        print(type(items))
        print(items)
        user_ids = ','.join(str(i) for i in items)
        print(user_ids)

        result = await self.get_users(user_ids)

        return result
