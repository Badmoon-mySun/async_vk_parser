import asyncio
import os

import requests as requests
from dotenv import load_dotenv, find_dotenv

from vk_api import VkAPI

load_dotenv(find_dotenv())

ACCESS_TOKEN = os.environ.get("VK_ACCESS_TOKEN")

vk = VkAPI(ACCESS_TOKEN, '5.131')

loop = asyncio.get_event_loop()
loop.run_until_complete(vk.test())
