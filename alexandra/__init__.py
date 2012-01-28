from pycassa.pool import ConnectionPool
from django.conf import settings

__version__ = (0,1)

pools = {}

for server_name, params in settings.CASSANDRA.iteritems():
	pools[server_name] = ConnectionPool(params['KEYSPACE'], params['CLUSTER'])

