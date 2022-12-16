from django.db import NotSupportedError
from django.contrib.gis.db.backends.base.operations import BaseSpatialOperations
from django.contrib.gis.db.backends.utils import SpatialOperator
from django_snowflake.operations import DatabaseOperations as SnowflakeDatabaseOperations

from .adapter import SnowflakeAdapter


class DWithinOperator(SpatialOperator):
    def __init__(self, func="ST_DWithin"):
        super().__init__(func=func)


class CoveredByOperator(SpatialOperator):
    def __init__(self, func="ST_COVEREDBY"):
        super().__init__(func=func)


class DatabaseOperations(SnowflakeDatabaseOperations, BaseSpatialOperations):
    # adapter for geometry/geography points
    Adapter = SnowflakeAdapter

    # Set of known unsupported functions of the backend
    unsupported_functions = {
    }

    gis_operators = {
        "dwithin": DWithinOperator(),
        "coveredby": CoveredByOperator(),
    }

    def last_executed_query(self, cursor, sql, params):
        # https://www.psycopg.org/docs/cursor.html#cursor.query
        # The query attribute is a Psycopg extension to the DB API 2.0.
        if cursor.query is not None:
            return cursor.query
        return None

    def geo_db_type(self, f):
        """
        Return the database field type for the given spatial field.
        """
        if f.geom_type == "RASTER":
            return "raster"

        if f.geography:
            if f.srid != 4326:
                raise NotSupportedError(
                    "PostGIS only supports geography columns with an SRID of 4326."
                )

            return "geography"
        else:
            return "geometry"

    def quote_name(self, name):
        if name.startswith('"') and name.endswith('"'):
            return name  # Quoting once is enough.
        return '"%s"' % name.upper().replace('.', '"."')

    def get_distance(self, f, value, lookup_type):
        """
        Return the distance parameters for the given geometry field,
        lookup value, and lookup type.
        """
        return [value[0].m]

    def get_geom_placeholder(self, f, value, compiler):
        if compiler.__class__.__name__ == "SQLInsertCompiler":
            # Casting to geography is not needed when inserting
            return "%s"
        return f"TO_GEOGRAPHY(%s)"
