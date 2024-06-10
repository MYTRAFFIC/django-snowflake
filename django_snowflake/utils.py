import django
from django.core.exceptions import ImproperlyConfigured
from django.utils.version import get_version_tuple


def check_django_compatability():
    """
    Verify that this version of django-snowflake is compatible with the
    installed version of Django. For example, any django-snowflake 3.x is
    compatible with Django 3.x.
    """
    from . import __version__
    if django.VERSION[0] != get_version_tuple(__version__)[0]:
        raise ImproperlyConfigured(
            'You must use the latest major version of django-snowflake {A}.x '
            'with Django {A}.x (found django-snowflake {B}).'.format(
                A=django.VERSION[0],
                B=__version__,
            )
        )
