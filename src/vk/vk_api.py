import aiohttp as aiohttp
import urllib.parse as urlparse

from settings import MAX_POSTS_COUNT


class VkAPI:
    VK_URL = 'https://vk.com'
    VK_API_URL = "https://api.vk.com"
    GROUP_MEMBERS_METHOD = 'groups.getMembers'
    USERS_GET_METHOD = 'users.get'
    WALL_GET_METHOD = 'wall.get'
    GROUPS_GET_GROUPS = 'groups.getById'
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

        return f'{self.VK_API_URL}/method/{method}?{params}'

    @staticmethod
    async def __request(uri: str, method='GET', body=None, headers=None) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.request(method, uri, json=body, headers=headers) as response:
                try:
                    return await response.json()
                except KeyError as e:
                    print(e)
                    return {'response': {}}

    async def request(self, api_method: str, params: dict, method: str = 'GET', body=None, headers=None):
        uri = self.__generate_uri(api_method, **params)
        response = await self.__request(uri, method, body, headers)

        try:
            return response['response']
        except KeyError:
            print(response)
            return {}

    async def get_group_members(self, group_id: str, offset: int = 0) -> dict:
        response = await self.request(self.GROUP_MEMBERS_METHOD, {'group_id': group_id, 'offset': offset})

        return response

    async def get_users(self, user_ids: str) -> dict:
        response = await self.request(self.USERS_GET_METHOD, {'user_ids': user_ids, 'fields': 'sex,bdate,contacts'})

        return response

    async def get_wall(self, domain, offset: int = 0, count: int = MAX_POSTS_COUNT):
        response = await self.request(self.WALL_GET_METHOD, {'domain': domain, 'offset': offset, 'count': count})
        print(f'posts offset - {offset}')

        return response

    async def get_posts_count(self, domain):
        response = await self.request(self.WALL_GET_METHOD, {'domain': domain, 'count': 0})

        return response

    async def get_groups(self, domains: str):
        response = await self.request(self.GROUPS_GET_GROUPS,
                                      {'group_ids': domains, 'fields': 'description,members_count'})

        return response
