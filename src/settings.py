import asyncio
import os

import requests as requests
from dotenv import load_dotenv, find_dotenv

from vk_api import VkAPI

load_dotenv(find_dotenv())

ACCESS_TOKEN = os.environ.get("VK_ACCESS_TOKEN")
API_VERSION = '5.131'
GROUP_MEMBER_OFFSET = 1000
