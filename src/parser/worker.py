import asyncio
import datetime

from db.models import *
from parser.services import save_items_to_datasets, save_metadata, save_indicator_value, save_indicator_newest, \
    get_id_storage_if_exist, update_district_indicators, save_new_districts_items, clean_post, \
    get_or_create_metadata, update_metadata, delete_dataset_by_id, set_indicators_rank, get_new_indicator_value_or_none
from settings import API_VERSION, GROUP_MEMBER_OFFSET, MAX_POSTS_COUNT, USER_ATTRIBUTES, POST_ATTRIBUTES
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

            if not group_members:
                break

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

            try:
                offset_vk += MAX_POSTS_COUNT
                if offset_vk >= wall_posts['count']:
                    break
            except KeyError as err:
                print(err)
                return []

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

        return len(items)

    async def __do_iteration(self, vk: VkAPI, groups: list, district_name: str, caption: str,
                             metadata: Metadata, async_get_items_func):
        district = Districts.get(Districts.Name == district_name)

        tasks = []
        for group in groups:
            offset = 0
            while offset < group['members_count']:
                task = asyncio.create_task(
                    self.__get_and_save_items(vk, metadata.Id, group['screen_name'], offset, 1000, async_get_items_func)
                )

                tasks.append(task)
                offset += 1000

        counts = await asyncio.gather(*tasks)
        count_items = sum(counts)

        try:
            indicator = Indicators.get_or_none(Indicators.DatasetId == metadata.Id)
            indicator_value = get_new_indicator_value_or_none(indicator.Id, district.Id) if indicator else None

            if indicator_value:
                print(f'metadata {metadata.Id} items count = {metadata.ItemsCount}')
                await update_district_indicators(indicator_value, count_items)
            else:
                print(f'metadata {metadata.Id} items count set = {count_items}')
                await save_new_districts_items(caption, metadata.Id, district, count_items, indicator)
        except ValueError as e:
            print(e)

    async def __do_work(self, vk: VkAPI, caption: str, description: str, data_type: str, async_get_items_func):
        metadata = get_or_create_metadata(data_type, caption, description, USER_ATTRIBUTES)
        delete_dataset_by_id(metadata.Id)

        for district, domains in self.data.items():
            try:
                groups = await self.__get_groups_info(vk, ','.join(domains))

                await self.__do_iteration(vk, groups, district, caption, metadata, async_get_items_func)
            except ValueError as e:
                print(e)

        update_metadata(metadata)
        set_indicators_rank(metadata.Id)

    async def do_work(self, vk: VkAPI):
        users_task = asyncio.create_task(
            self.__do_work(vk, 'Аудитория пользователей Вконтакте', 'Участники сообществ по районам Москвы',
                           'users', self.__get_group_users)
        )

        posts_task = asyncio.create_task(
            self.__do_work(vk, 'Количество публикаций в Вконтакте', 'Посты сообществ по районам Москвы',
                           'posts', self.__get_group_posts)
        )

        await asyncio.gather(users_task, posts_task)

    def start_worker(self):
        vk = VkAPI(self.vk_token, API_VERSION)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.do_work(vk))
