from db.models import *
import datetime

from parser.worker import Worker
from settings import ACCESS_TOKEN

if __name__ == '__main__':
    try:
        data = {
            'район Выхино-Жулебино': [
                'vykhino_zhulebino_online',
                # 'vyhino_zhulebino_online',
                # 'uvao_vihino',
                # 'vihino_julebino_nekrasovka'
            ]
        }

        worker = Worker(ACCESS_TOKEN, data)
        worker.start_worker()
    except ValueError as e:
        print(e)

    # metadata = Metadata(Id=10123, DatasetSource='vk', IdentificationNumber='', CategoryId=0, CategoryCaption='',
    #                     Category1LevelCaption='', DepartmentId=0, DepartmentCaption='', Caption='caption',
    #                     Description='description', Keywords='kewords', ContainsGeodata=False, VersionNumber='1',
    #                     VersionDate=datetime.date.today(), ItemsCount=1, Attributes='[{"help": "some json text"}]')
    # metadata.save()
