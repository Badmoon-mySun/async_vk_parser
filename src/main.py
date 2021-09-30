from db.models import *

from parser.worker import Worker
from settings import ACCESS_TOKEN, _json_files_dir

if __name__ == '__main__':
    with open(os.path.join(_json_files_dir, 'groups2.json')) as data:
        groups = json.loads(data.read())

        try:
            worker = Worker(ACCESS_TOKEN, groups)
            worker.start_worker()
        except ValueError as e:  # TODO заменить ValueError на Exception везде
            print(e)
