from db.models import *
import datetime

if __name__ == '__main__':
    # try:
    #     worker = Worker(ACCESS_TOKEN, ['itis_studactive'])
    #     worker.start_worker()
    # except Exception as e:
    #     database.close_connection()
    #     print(e)

    metadata = Metadata(Id=10123, DatasetSource='vk', IdentificationNumber='1', CategoryId=12,
                        CategoryCaption='category caption 2 level', Category1LevelCaption='category caption 1 level',
                        DepartmentId=12, DepartmentCaption='departament caption', Caption='caption',
                        Description='description', Keywords='kewords', ContainsGeodata=False, VersionNumber='1',
                        VersionDate=datetime.date.today(), ItemsCount=1, Attributes='[{"help": "some json text"}]')
    metadata.save()
