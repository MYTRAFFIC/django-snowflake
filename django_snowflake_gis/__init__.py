__version__ = '4.0a1'

# Check Django compatibility before other imports which may fail if the
# wrong version of Django is installed.
from django_snowflake.utils import check_django_compatability

check_django_compatability()

from .functions import register_functions

register_functions()
