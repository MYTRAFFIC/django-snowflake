from django_snowflake.base import DatabaseWrapper as SnowflakeDatabaseWrapper
from django.utils.asyncio import async_unsafe

from .features import DatabaseFeatures
from .operations import DatabaseOperations
from .adapter import SnowflakeAdapter


class DatabaseWrapper(SnowflakeDatabaseWrapper):
    features_class = DatabaseFeatures
    ops_class = DatabaseOperations

    @async_unsafe
    def get_new_connection(self, conn_params):
        connection = super().get_new_connection(conn_params)
        setattr(connection.converter, f"_{SnowflakeAdapter.__name__.lower()}_to_snowflake", str)
        return connection
