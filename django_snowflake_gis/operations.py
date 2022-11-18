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

    def bulk_insert_sql(self, fields, placeholder_rows):
        placeholder_rows_sql = (", ".join(row) for row in placeholder_rows)
        values_sql = ", ".join("(%s)" % sql for sql in placeholder_rows_sql)
        field_db_types = [field.db_type(self.connection) for field in fields]
        if "VARIANT" in field_db_types:
            """
            When inserting VARIANT, OBJECT or ARRAY values, the "INSERT INTO ... SELECT"
            format is required. PARSE_JSON(), TO_VARIANT() and other functions like
            these cannot be used in "INSERT INTO ... VALUES (...)". Read more:
            https://docs.snowflake.com/en/sql-reference/data-types-semistructured.html#example-of-inserting-a-variant
            
            Ex:
            INSERT INTO my_table(col1, col2, col3)
            VALUES (TO_VARIANT(val1), PARSE_JSON(val2), val3)
            
             |
             |  becomes
             V
            
            INSERT INTO my_table(col1, col2, col3)
            SELECT
                TO_VARIANT(Column1) as col1,
                PARSE_JSON(Column2) as col2,
                Column3 as col3
            FROM VALUES (val1, val2, val3)
            """
            columns = [
                f'Column{index + 1}' if field.__class__.__name__ != "JSONField"
                else f'PARSE_JSON(Column{index + 1})'
                for index, field in enumerate(fields)
            ]
            field_casts = [
                f'{columns[index]} AS {field.name}' if field_db_types[index] != "VARIANT"
                else f'TO_VARIANT({columns[index]}) AS {field.name}'
                for index, field in enumerate(fields)
            ]
            return f"SELECT {', '.join(field for field in field_casts)} FROM VALUES {values_sql}"
        return "VALUES " + values_sql
