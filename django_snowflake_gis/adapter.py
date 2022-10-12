from django.contrib.gis.db.backends.base.adapter import WKTAdapter

from psycopg2 import Binary
from psycopg2.extensions import ISQLQuote

from django.contrib.gis.db.backends.postgis.pgraster import to_pgraster
from django.contrib.gis.geos import GEOSGeometry


class SnowflakeAdapter(WKTAdapter):
    def __init__(self, obj, geography=True):
        """
        Initialize on the spatial object.
        """
        self.is_geometry = isinstance(obj, (GEOSGeometry, SnowflakeAdapter))

        # Getting the WKB (in string form, to allow easy pickling of
        # the adaptor) and the SRID from the geometry or raster.
        if self.is_geometry:
            self.ewkb = bytes(obj.ewkb)
            self._adapter = Binary(self.ewkb)
        else:
            self.ewkb = to_pgraster(obj)

        self.obj = obj
        self.srid = obj.srid
        self.geography = geography

    def __conform__(self, proto):
        """Does the given protocol conform to what Psycopg2 expects?"""
        if proto == ISQLQuote:
            return self
        else:
            raise Exception(
                "Error implementing psycopg2 protocol. Is psycopg2 installed?"
            )

    def __eq__(self, other):
        return isinstance(other, SnowflakeAdapter) and self.ewkb == other.ewkb

    def __hash__(self):
        return hash(self.ewkb)

    def __str__(self):
        return self.getquoted()

    @classmethod
    def _fix_polygon(cls, poly):
        return poly

    def getquoted(self):
        if self.is_geometry:
            return str(self.obj)
        else:
            # For rasters, add explicit type cast to WKB string.
            return "'%s'::raster" % self.ewkb.encode()
