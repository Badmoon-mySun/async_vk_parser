import json

from peewee import *

from settings import DB_NAME, DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT

db = MySQLDatabase(DB_NAME, user=DB_USERNAME, passwd=DB_PASSWORD, host=DB_HOST, port=DB_PORT)


class JSONField(TextField):
    def db_value(self, value):
        return json.dumps(value)

    def python_value(self, value):
        if value is not None:
            return json.loads(value)


class Metadata(Model):
    Id = IntegerField()
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
        database = db
        table_name = 'metadata'


class Datasets(Model):
    DatasetId = ForeignKeyField(model=Metadata, field='Id')
    SubdatasetId = IntegerField()
    DatasetSource = CharField(max_length=20, default='data.mos.ru')
    jdoc = JSONField()

    class Meta:
        database = db
        table_name = 'datasets'


class Indicators(Model):
    Id = AutoField(unique=True)
    Name = TextField()
    Type = TextField(null=True)
    DatasetId = ForeignKeyField(model=Metadata, field='Id')
    Positive = BooleanField(null=True)
    Info = TextField(null=True)

    class Meta:
        database = db
        table_name = 'indicators'


class AdmAreas(Model):
    Id = AutoField(unique=True)
    Name = CharField(max_length=50, unique=True)

    class Meta:
        database = db
        table_name = 'adm_areas'


class Districts(Model):
    Id = AutoField(unique=True)
    AreaId = ForeignKeyField(model=AdmAreas, field='Id')
    Name = CharField(max_length=50)

    class Meta:
        database = db
        table_name = 'districts'


class IndicatorsValues(Model):
    Id = AutoField(unique=True)
    IndicatorId = ForeignKeyField(model=Indicators, field='Id')
    AreaId = ForeignKeyField(model=AdmAreas, field='Id')
    DistrictId = ForeignKeyField(model=Districts, field='Id')
    Value = FloatField(null=True)
    PrevValue = FloatField(null=True)
    TerritoryRank = SmallIntegerField(null=True)
    PrevTerritoryRank = SmallIntegerField(null=True)
    VersionNumber = CharField(max_length=50)
    VersionDate = DateField()
    IsNewest = BooleanField(default=False)

    class Meta:
        database = db
        table_name = 'indicators_values'


class IndicatorsValuesNewest(Model):
    Id = AutoField(unique=True)
    IndicatorId = ForeignKeyField(model=Indicators, field='Id')
    AreaId = ForeignKeyField(model=AdmAreas, field='Id')
    DistrictId = ForeignKeyField(model=Districts, field='Id')
    Value = FloatField(null=True)
    PrevValue = FloatField(null=True)
    TerritoryRank = SmallIntegerField(null=True)
    PrevTerritoryRank = SmallIntegerField(null=True)
    VersionNumber = CharField(max_length=50)
    VersionDate = DateField()

    class Meta:
        database = db
        table_name = 'indicators_values_newest'
