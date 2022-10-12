from django.contrib.gis.db.backends.base.features import BaseSpatialFeatures

from django_snowflake.features import DatabaseFeatures as SnowflakeFeatures


class DatabaseFeatures(SnowflakeFeatures, BaseSpatialFeatures):
    has_spatialrefsys_table = False
    supports_geography = True
