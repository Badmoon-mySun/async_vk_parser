import os

from dotenv import load_dotenv, find_dotenv
from peewee import MySQLDatabase

load_dotenv(find_dotenv())

ACCESS_TOKEN = os.environ.get("VK_ACCESS_TOKEN")
API_VERSION = '5.131'

GROUP_MEMBER_OFFSET = 1000
MAX_POSTS_COUNT = 100

DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

print(DB_USERNAME, DB_PASSWORD, DB_PORT, DB_HOST, DB_NAME)

