import asyncio

from settings import API_VERSION, GROUP_MEMBER_OFFSET, ACCESS_TOKEN, MAX_POSTS_COUNT
from vk_api import VkAPI


class Worker:
    vk_token: str
    domains: list

    def __init__(self, vk_token, domains):
        self.vk_token = vk_token
        self.domains = domains

    async def do_work(self, vk: VkAPI):
        for domain in self.domains:

            offset = 0
            while False:
                group_members = await vk.get_group_members(domain, offset)
                users_ids = group_members['items']

                users = await vk.get_users(users_ids) # TODO save users

                offset += GROUP_MEMBER_OFFSET
                if offset >= group_members['count']:
                    break

            offset = 0
            while True:
                wall_posts = await vk.get_wall(domain, offset)

                offset += MAX_POSTS_COUNT
                if offset >= wall_posts['count']:
                    break

    def start_worker(self):
        vk = VkAPI(self.vk_token, API_VERSION)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.do_work(vk))
