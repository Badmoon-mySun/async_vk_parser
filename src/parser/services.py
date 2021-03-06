import asyncio
import datetime

from typing import List

from db.models import Metadata, mysql_db, Datasets, Districts, IndicatorsValues, IndicatorsValuesNewest, IdStorage, \
    Indicators
from peewee import IntegrityError, fn

from settings import DATA_SOURCE


def save_metadata(caption: str, description: str, items_cont: int, attributes: list):
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
        last_metadata = Metadata.select().order_by(Metadata.Id.desc()).limit(1)

        if last_metadata:
            last_metadata_id = last_metadata[0].Id
        else:
            last_metadata_id = 0

        try:
            with mysql_db.atomic():
                metadata_id = last_metadata_id + 1
                metadata.Id = metadata_id
        except IntegrityError:
            metadata_id = 0

    metadata.save(force_insert=True)
    print('metadata save')
    print(metadata.Id)

    return metadata


def save_items_to_datasets(metadata_id: int, elements: list):
    if elements:
        last_datasets = Datasets.select(Datasets.SubdatasetId).where(
            Datasets.DatasetId == metadata_id).order_by(Datasets.SubdatasetId.desc()).limit(1).scalar()

        if last_datasets:
            last_dataset_sub_id = last_datasets + 1
        else:
            last_dataset_sub_id = 1

        Datasets.create(
            DatasetId=metadata_id,
            SubdatasetId=last_dataset_sub_id,
            DatasetSource=DATA_SOURCE,
            jdoc=elements
        )


def save_indicator_value(indicator_id: int, district: Districts, items_count: int):
    indicator_args = {
        'IndicatorId': indicator_id,
        'AreaId': district.AreaId,
        'DistrictId': district.Id,
        'Value': items_count,
        'PrevValue': 0,
        'VersionNumber': '1',
        'VersionDate': datetime.date.today(),
        'PrevTerritoryRank': 0,
        'TerritoryRank': 0,
    }

    indicator_value = IndicatorsValues(IsNewest=True, **indicator_args)
    indicator_value.save()

    return indicator_value


def save_indicator_newest(indicator_value: IndicatorsValues):
    return IndicatorsValuesNewest.create(
        Id=indicator_value.Id,
        IndicatorId=indicator_value.IndicatorId,
        AreaId=indicator_value.AreaId,
        DistrictId=indicator_value.DistrictId,
        Value=indicator_value.Value,
        PrevValue=indicator_value.PrevValue,
        VersionNumber=indicator_value.VersionNumber,
        PrevTerritoryRank=indicator_value.PrevTerritoryRank,
        VersionDate=indicator_value.VersionDate
    )


def get_id_storage_if_exist(data_type: str) -> IdStorage:
    return IdStorage.get_or_none(IdStorage.data_type == data_type)


def get_new_indicator_value_or_none(indicator_id: int, district_id: int) -> IndicatorsValues:
    indicator_values = IndicatorsValues.select().where(
        IndicatorsValues.DistrictId == district_id,
        IndicatorsValues.IndicatorId == indicator_id,
        IndicatorsValues.IsNewest == True
    ).order_by(IndicatorsValues.Id.desc()).limit(1).execute()

    return indicator_values[0] if indicator_values else None


async def update_district_indicators(indicator_value: IndicatorsValues, count_items):
    indicator_value.IsNewest = False
    indicator_value.save()

    indicator_args = {
        'IndicatorId': indicator_value.IndicatorId,
        'AreaId': indicator_value.AreaId,
        'DistrictId': indicator_value.DistrictId,
        'Value': count_items,
        'PrevValue': indicator_value.Value,
        'VersionNumber': update_version(indicator_value.VersionNumber),
        'VersionDate': indicator_value.VersionDate,
        'PrevTerritoryRank': indicator_value.TerritoryRank,
        'TerritoryRank': 0,
        'IsNewest': True
    }

    new_indicator_value = IndicatorsValues(**indicator_args)
    new_indicator_value.save()

    IndicatorsValuesNewest.delete().where(
        IndicatorsValuesNewest.DistrictId == indicator_value.DistrictId,
        IndicatorsValuesNewest.IndicatorId == indicator_value.IndicatorId,
    ).execute()

    save_indicator_newest(new_indicator_value)


async def save_new_districts_items(caption: str, metadata_id: int, district: Districts, count_items: int,
                                   indicator: Indicators):
    if not indicator:
        indicator = Indicators.create(Name='', DatasetId=metadata_id, Info=caption)

    indicator_value = save_indicator_value(indicator.Id, district, count_items)
    save_indicator_newest(indicator_value)


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


def get_or_create_metadata(data_type: str, caption: str, desc: str, attrs: list) -> Metadata:
    id_storage = get_id_storage_if_exist(data_type)

    if id_storage:
        metadata = Metadata.get(Metadata.Id == id_storage.metadata_id)
    else:
        metadata = save_metadata(caption, desc, 0, attrs)

        IdStorage.create(data_type=data_type, metadata_id=metadata.Id)

    return metadata


def delete_dataset_by_id(dataset_id: int):
    Datasets.delete().where(Datasets.DatasetId == dataset_id).execute()


def update_version(version: str) -> str:
    return str(round(float(version) + 0.1, 1))


def get_items_count_by_indicator_id(indicator_id: int) -> int:
    return IndicatorsValuesNewest.select(
        fn.SUM(IndicatorsValuesNewest.Value)).where(IndicatorsValuesNewest.IndicatorId == indicator_id).scalar()


def update_metadata(metadata: Metadata):
    indicator = Indicators.get(Indicators.DatasetId == metadata.Id)

    # ???????? ???????????????? ?????? ???? ????????????????????, ???????????? ?????? ?????????? metadata
    if metadata.ItemsCount != 0:
        metadata.VersionNumber = update_version(metadata.VersionNumber)

    metadata.VersionDate = datetime.date.today()

    # ?????????? ???????????? ???????????????????? ???????? ?????????????????? ???????????????????? ???????????????? ???????? ??????????????????????
    metadata.ItemsCount = get_items_count_by_indicator_id(indicator.Id)
    metadata.save()


def set_indicators_rank(metadata_id: int):
    indicator = Indicators.get(Indicators.DatasetId == metadata_id)
    indicators_values_old = {}

    indicators_values = IndicatorsValues.filter(
        IndicatorsValues.IndicatorId == indicator.Id,
        IndicatorsValues.IsNewest == True
    )

    id_rank_tuples = IndicatorsValues.select(
        IndicatorsValues.IndicatorId,
        IndicatorsValues.TerritoryRank
    ).where(
        IndicatorsValues.IndicatorId == indicator.Id,
        IndicatorsValues.IsNewest == False
    ).order_by(IndicatorsValues.Id.desc()).tuples()

    if id_rank_tuples:
        print(id_rank_tuples)
        for district_id, ter_rank in id_rank_tuples:
            indicators_values_old[district_id] = ter_rank

    sorted(indicators_values, key=lambda v: v.Value)

    last_value, rank = -1, 1
    for indicator_value in indicators_values:
        indicator_value_newest = IndicatorsValuesNewest.get(
            IndicatorsValuesNewest.Id == indicator_value.Id
        )

        if indicator_value.TerritoryRank is not None and indicator_value.TerritoryRank == last_value:
            rank -= 1
        else:
            last_value = indicator_value.Value

        indicator_value.PrevTerritoryRank = indicators_values_old.get(indicator_value.IndicatorId.Id, 0)
        indicator_value.TerritoryRank = rank

        indicator_value_newest.PrevTerritoryRank = indicator_value.PrevTerritoryRank
        indicator_value_newest.TerritoryRank = indicator_value.TerritoryRank

        indicator_value.save()
        indicator_value_newest.save()
        rank += 1
