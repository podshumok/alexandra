import pycassa
from pycassa import types
from django.conf import settings

__version__ = (0,2)

pools = dict( ((pool_name, None) for pool_name in settings.CASSANDRA) )

Pool = pycassa.ConnectionPool
CF = pycassa.ColumnFamily
