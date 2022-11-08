from django.contrib.gis.db.models.functions import AsGeoJSON, Distance, Length, Perimeter
from django_snowflake.functions import register_functions as snowflake_register_functions


def snowflake_distance(self, compiler, connection, **extra_context):
    clone = self.copy()
    function = connection.ops.spatial_function_name("ST_Distance")

    return super(Distance, clone).as_sql(
        compiler, connection, function=function, **extra_context
    )


def snowflake_geojson(self, compiler, connection, **extra_context):
    source_expressions = self.get_source_expressions()
    clone = self.copy()
    clone.set_source_expressions(source_expressions[:1])

    function = connection.ops.spatial_function_name("ST_ASGEOJSON")

    return super(AsGeoJSON, clone).as_sql(
        compiler, connection, function=function, **extra_context
    )


def register_functions():
    snowflake_register_functions()

    Distance.as_snowflake = snowflake_distance
    AsGeoJSON.as_snowflake = snowflake_geojson
    Length.as_snowflake = Length.as_postgresql
    Perimeter.as_snowflake = Perimeter.as_postgresql
