import asyncio
import datetime

from db.models import Metadata, mysql_db, Datasets, Districts, IndicatorsValues, IndicatorsValuesNewest, IdStorage, \
    Indicators
from peewee import IntegrityError

DATA_SOURCE = 'vk.com'


def save_metadata(description: str, caption: str, items_cont: int, attributes: list):
    metadata_ags = {
        'DatasetSource': DATA_SOURCE,
        'IdentificationNumber': '',
        'CategoryId': 0,
        'CategoryCaption': '',
        'Category1LevelCaption': '',
        'DepartmentId': 0,
        'DepartmentCaption': '',
        'Caption': caption,
        'Description': description,
        'Keywords': '',
        'ContainsGeodata': False,
        'VersionNumber': '1',
        'VersionDate': datetime.date.today(),
        'ItemsCount': items_cont,
        'Attributes': attributes
    }

    metadata = Metadata(**metadata_ags)

    metadata_id = 0
    while not metadata_id:
        last_metadata = Metadata.select().order_by(Metadata.Id.desc()).limit(1)[0]
        try:
            with mysql_db.atomic():
                metadata_id = last_metadata.Id + 1
                metadata.Id = metadata_id
        except IntegrityError:
            metadata_id = 0

    metadata.save(force_insert=True)
    print('metadata save')

    print(metadata.Id)

    return metadata


def save_items_to_datasets(metadata_id: int, elements: list):
    if elements:
        dataset = Datasets(DatasetId=metadata_id, SubdatasetId=0, DatasetSource=DATA_SOURCE, jdoc=elements)
        dataset.save()

        return dataset
    else:
        return None


def save_indicator_value(indicator_id: int, district: Districts, items_count: int):
    indicator_args = {
        'IndicatorId': indicator_id,
        'AreaId': district.AreaId,
        'DistrictId': district.Id,
        'Value': items_count,
        'PrevValue': 0,
        'VersionNumber': '1',
        'VersionDate': datetime.date.today(),
    }

    indicator_value = IndicatorsValues(IsNewest=True, **indicator_args)
    indicator_value.save()

    return indicator_value


def save_indicator_newest(indicator_id: int, indicator_value_id: int, district: Districts, items_count: int):
    indicator_value_newest = IndicatorsValuesNewest(
        Id=indicator_value_id,
        IndicatorId=indicator_id,
        AreaId=district.AreaId,
        DistrictId=district.Id,
        Value=items_count,
        PrevValue=0,
        VersionNumber='1',
        VersionDate=datetime.date.today()
    )

    indicator_value_newest.save()

    return indicator_value_newest


def get_id_storage_if_exist(screen_name: str, data_type: str) -> IdStorage:
    return IdStorage.get_or_none(IdStorage.screen_name == screen_name, IdStorage.data_type == data_type)


async def update_group(id_storage: IdStorage, tasks: list, value: int):
    metadata = Metadata.get(Metadata.Id == id_storage.metadata_id)
    metadata.VersionNumber = round(id_storage.version + 0.1, 1)
    metadata.VersionDate = datetime.date.today()
    metadata.ItemsCount = value
    metadata.save()

    Datasets.delete().where(Datasets.DatasetId == id_storage.dataset_id).execute()

    await asyncio.gather(*tasks)

    indicator_value_newest = IndicatorsValuesNewest.get(
        IndicatorsValuesNewest.Id == id_storage.newest_indicator_value_id)
    indicator_value_newest.PrevValue = indicator_value_newest.Value
    indicator_value_newest.Value = value
    indicator_value_newest.VersionNumber = metadata.VersionNumber
    indicator_value_newest.VersionDate = metadata.VersionDate
    indicator_value_newest.save()

    indicator_value_old = IndicatorsValues.get(IndicatorsValues.Id == id_storage.last_indicator_value_id)
    indicator_value_old.IsNewest = False
    indicator_value_old.save()

    indicator_args = {
        'IndicatorId': id_storage.indicator_id,
        'AreaId': indicator_value_newest.AreaId,
        'DistrictId': indicator_value_newest.DistrictId,
        'Value': value,
        'PrevValue': indicator_value_newest.PrevValue,
        'VersionNumber': metadata.VersionNumber,
        'VersionDate': datetime.date.today(),
        'IsNewest': True
    }

    indicator_value = IndicatorsValues(**indicator_args)
    indicator_value.save()

    id_storage.last_indicator_value_id = indicator_value.Id
    id_storage.version = indicator_value.VersionNumber
    id_storage.value = value
    id_storage.save()


async def save_new_group(group: dict, tasks: list, caption: str, district_name: str, data_type: str, metadata_id: int):
    count = group['members_count']
    description = group['description']

    await asyncio.gather(*tasks)
    print('gather')

    indicator = Indicators(Name=caption, DatasetId=metadata_id, Info=description)
    indicator.save()

    district = Districts.get(Districts.Name == district_name)

    indicator_value = save_indicator_value(indicator.Id, district, count)
    indicator_value_newest = save_indicator_newest(indicator.Id, indicator_value.Id, district, count)

    id_storage = IdStorage(
        screen_name=group['screen_name'],
        data_type=data_type,
        metadata_id=metadata_id,
        dataset_id=metadata_id,
        indicator_id=indicator.Id,
        last_indicator_value_id=indicator_value.Id,
        newest_indicator_value_id=indicator_value_newest.Id,
        version=1.0,
        value=count
    )

    id_storage.save()


def clean_post(post: dict) -> dict:
    return {
        'id': post.get('id', ''),
        'owner_id': post.get('owner_id', ''),
        'from_id': post.get('from_id', ''),
        'date': post.get('date', ''),
        'text': post.get('text', ''),
        'comments_count': post.get('comments', {}).get('count', ''),
        'likes_count': post.get('likes', {}).get('count', ''),
        'reposts_count': post.get('reposts', {}).get('count', ''),
        'views_count': post.get('views', {}).get('count', '')
    }

