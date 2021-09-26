import json
import os.path

from peewee import *

from settings import DB_NAME, DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, ROOT_DIR

mysql_db = MySQLDatabase(DB_NAME, user=DB_USERNAME, passwd=DB_PASSWORD, host=DB_HOST, port=DB_PORT)

sqlite_db = SqliteDatabase(os.path.join(ROOT_DIR, 'sqlite.db'),
                           pragmas={
                               'journal_mode': 'wal',
                               'cache_size': -1024 * 64}
                           )


class JSONField(TextField):
    def db_value(self, value):
        return json.dumps(value)

    def python_value(self, value):
        if value is not None:
            return json.loads(value)


class Metadata(Model):
    Id = IntegerField(primary_key=True, column_name='Id')
    DatasetSource = CharField(max_length=20, default='data.mos.ru')
    IdentificationNumber = TextField()
    CategoryId = IntegerField()
    CategoryCaption = TextField()
    Category1LevelCaption = TextField(null=True)
    DepartmentId = IntegerField()
    DepartmentCaption = TextField()
    Caption = TextField()
    Description = TextField()
    Keywords = TextField()
    ContainsGeodata = BooleanField()
    VersionNumber = TextField()
    VersionDate = DateField()
    ItemsCount = IntegerField()
    Attributes = JSONField()

    class Meta:
        database = mysql_db
        table_name = 'metadata'


class Datasets(Model):
    DatasetId = ForeignKeyField(model=Metadata, to_field='Id', column_name='DatasetId', primary_key=True)
    SubdatasetId = IntegerField()
    DatasetSource = CharField(max_length=20, default='data.mos.ru')
    jdoc = JSONField()

    class Meta:
        database = mysql_db
        table_name = 'datasets'


class Indicators(Model):
    Id = AutoField(unique=True)
    Name = TextField()
    Type = TextField(null=True)
    DatasetId = ForeignKeyField(model=Datasets, to_field='DatasetId', column_name='DatasetId')
    Positive = BooleanField(null=True)
    Info = TextField(null=True)

    class Meta:
        database = mysql_db
        table_name = 'indicators'


class AdmAreas(Model):
    Id = AutoField(unique=True)
    Name = CharField(max_length=50, unique=True)

    class Meta:
        database = mysql_db
        table_name = 'adm_areas'


class Districts(Model):
    Id = AutoField(unique=True)
    AreaId = ForeignKeyField(model=AdmAreas, to_field='Id', column_name='AreaId')
    Name = CharField(max_length=50)

    class Meta:
        database = mysql_db
        table_name = 'districts'


class IndicatorsValues(Model):
    Id = AutoField(unique=True)
    IndicatorId = ForeignKeyField(model=Indicators, to_field='Id', column_name='IndicatorId')
    AreaId = ForeignKeyField(model=AdmAreas, to_field='Id', column_name='AreaId')
    DistrictId = ForeignKeyField(model=Districts, to_field='Id', column_name='DistrictId')
    Value = FloatField(null=True)
    PrevValue = FloatField(null=True)
    TerritoryRank = SmallIntegerField(null=True)
    PrevTerritoryRank = SmallIntegerField(null=True)
    VersionNumber = CharField(max_length=50)
    VersionDate = DateField()
    IsNewest = BooleanField(default=False)

    class Meta:
        database = mysql_db
        table_name = 'indicators_values'


class IndicatorsValuesNewest(Model):
    Id = ForeignKeyField(model=IndicatorsValues, to_field='Id', column_name='Id')
    IndicatorId = ForeignKeyField(model=Indicators, to_field='Id', column_name='IndicatorId')
    AreaId = ForeignKeyField(model=AdmAreas, to_field='Id', column_name='AreaId')
    DistrictId = ForeignKeyField(model=Districts, to_field='Id', column_name='DistrictId')
    Value = FloatField(null=True)
    PrevValue = FloatField(null=True)
    TerritoryRank = SmallIntegerField(null=True)
    PrevTerritoryRank = SmallIntegerField(null=True)
    VersionNumber = CharField(max_length=50)
    VersionDate = DateField()

    class Meta:
        database = mysql_db
        table_name = 'indicators_values_newest'


class IdStorage(Model):
    id = AutoField(unique=True)
    data_type = CharField()
    metadata_id = IntegerField(unique=True)

    class Meta:
        database = sqlite_db
        table_name = 'id_storage'


if not sqlite_db.table_exists('id_storage'):
    sqlite_db.create_tables([IdStorage])
