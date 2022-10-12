from django.contrib.gis.db.models.functions import Distance, Length, Perimeter
from django_snowflake.functions import register_functions as snowflake_register_functions


def snowflake_distance(self, compiler, connection, **extra_context):
    clone = self.copy()
    function = connection.ops.spatial_function_name("ST_Distance")

    return super(Distance, clone).as_sql(
        compiler, connection, function=function, **extra_context
    )


def register_functions():
    snowflake_register_functions()

    Distance.as_snowflake = snowflake_distance
    Length.as_snowflake = Length.as_postgresql
    Perimeter.as_snowflake = Perimeter.as_postgresql
