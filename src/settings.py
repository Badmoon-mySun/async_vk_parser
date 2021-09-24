import json
import os

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

ACCESS_TOKEN = os.environ.get("VK_ACCESS_TOKEN")
API_VERSION = '5.131'

GROUP_MEMBER_OFFSET = 1000
MAX_POSTS_COUNT = 100

DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

_json_files_dir = os.path.join(ROOT_DIR, 'json')

with open(os.path.join(_json_files_dir, 'user_attributes.json'), 'r') as user_attributes:
    USER_ATTRIBUTES = json.loads(user_attributes.read())

with open(os.path.join(_json_files_dir, 'post_attributes.json'), 'r') as post_attributes:
    POST_ATTRIBUTES = json.loads(post_attributes.read())

DATA_SOURCE = 'vk.com'
