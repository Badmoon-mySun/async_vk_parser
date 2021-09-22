import asyncio
import datetime

from db.models import *
from parser.sevrvices import save_items_to_datasets, save_metadata, save_indicator_value, save_indicator_newest, \
    get_id_storage_if_exist, update_group, save_new_group, clean_post
from settings import API_VERSION, GROUP_MEMBER_OFFSET, MAX_POSTS_COUNT
from vk.vk_api import VkAPI


class Worker:
    vk_token: str
    data: dict

    def __init__(self, vk_token, data):
        self.vk_token = vk_token
        self.data = data

    @staticmethod
    async def __get_groups_info(vk: VkAPI, domains: str) -> list:
        return await vk.get_groups(domains)

    @staticmethod
    async def __get_group_users(vk: VkAPI, domain: str, offset: int, count: int) -> list:
        offset_vk = offset
        users = []

        while offset_vk < offset + count:
            group_members = await vk.get_group_members(domain, offset_vk)
            users += await vk.get_users(group_members['items'])
            print(f'user offset - {offset_vk}')

            offset_vk += GROUP_MEMBER_OFFSET
            if offset_vk >= group_members['count']:
                return users

        return users

    @staticmethod
    async def __get_group_posts(vk: VkAPI, domain: str, offset: int, count: int) -> list:
        offset_vk = MAX_POSTS_COUNT + offset
        posts = []
        tasks = []

        wall_posts = await vk.get_wall(domain, offset_vk)
        while offset_vk < offset + count:
            task = asyncio.create_task(vk.get_wall(domain, offset_vk))
            tasks.append(task)

            offset_vk += MAX_POSTS_COUNT
            if offset_vk >= wall_posts['count']:
                break

        posts += wall_posts['items']

        if tasks:
            tasks_wall_posts = await asyncio.gather(*tasks)
            for wall_post in tasks_wall_posts:
                posts += wall_post['items']

        return [clean_post(post) for post in posts]

    @staticmethod
    async def __get_and_save_items(vk: VkAPI, metadata_id: int, domain: str, offset: int, count: int,
                                   async_get_items_func):
        items = await async_get_items_func(vk, domain, offset, count)
        save_items_to_datasets(metadata_id, items[count // 2:])
        save_items_to_datasets(metadata_id, items[:count // 2])

    async def __do_iteration(self, vk: VkAPI, group: dict, district_name: str, data_type: str, attribute: list,
                             caption: str, count: int, async_get_items_func):

        id_storage = get_id_storage_if_exist(group['screen_name'], data_type)
        description = group['description']
        domain = group['screen_name']

        metadata_id = id_storage.metadata_id if id_storage else save_metadata(description, caption, count, attribute).Id
        print(f'metada id {metadata_id}')

        tasks = []
        offset = 0
        while offset < count:
            task = asyncio.create_task(
                self.__get_and_save_items(vk, metadata_id, domain, offset, 1000, async_get_items_func)
            )

            tasks.append(task)
            offset += 1000

        try:
            if id_storage:
                await update_group(id_storage, tasks, count)
            else:
                await save_new_group(group, tasks, caption, district_name, data_type, metadata_id)
        except ValueError as e:
            print(e)

    async def do_work(self, vk: VkAPI):
        for district, domains in self.data.items():
            try:
                groups = await self.__get_groups_info(vk, ','.join(domains))
                for group in groups:
                    posts_count = await vk.get_posts_count(group['screen_name'])
                    user_caption = f'Аудитория пользователей \"{group["name"]}\"'
                    post_caption = f'Посты сообщества \"{group["name"]}\"'

                    first_task = asyncio.create_task(
                        self.__do_iteration(vk, group, district, 'users', [{}], user_caption, group['members_count'], self.__get_group_users)
                    )
                    second_task = asyncio.create_task(
                        self.__do_iteration(vk, group, district, 'posts', [{}], post_caption, posts_count['count'], self.__get_group_posts)
                    )

                    await asyncio.gather(first_task, second_task)
            except ValueError as e:
                print(e)

    def start_worker(self):
        vk = VkAPI(self.vk_token, API_VERSION)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.do_work(vk))
