import pycassa
import alexandra

from django.conf import settings
from django.core.management.base import NoArgsCommand, ImproperlyConfigured
from django.db import models

class EmptyPool(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def __getattr__(self, name):
        return None

class EmptyCF(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def load_schema():
        return None

    def __getattr__(self, name):
        return None

alexandra.Pool = EmptyPool
alexandra.CF = EmptyCF

try:
    ROUTES = settings.CASSANDRA_ROUTES
except AttributeError:
    ROUTES = {'default': '*'}

class Command(NoArgsCommand):

    def handle_noargs(self, **options):
        if 'CASSANDRA' not in dir(settings):
            raise ImproperlyConfigured('Please add CASSANDRA definition to your settings.py\n')
        for pool_name, params in settings.CASSANDRA.iteritems():
            sys = pycassa.SystemManager(params['CLUSTER'][0])

            if pool_name not in ROUTES: continue # no apps asigned for this cluster-keyspace

            if params['KEYSPACE'] not in sys.list_keyspaces():
                sys.create_keyspace(
                    params['KEYSPACE'],
                    params.get('STRATEGY', pycassa.SIMPLE_STRATEGY),
                    params.get('STRATEGY_OPTIONS', {'replication_factor': '1'})
                )

            apps = []
            if ROUTES[pool_name] == '*':
                apps = models.get_apps()
            else:
                for app in ROUTES[pool_name]:
                    apps.append(models.get_app(app))

            for app in apps:
                for obj in dir(app):
                    obj = getattr(app, obj)
                    try:
                        from alexandra import cass
                        if not issubclass(obj, cass.ColumnFamily): continue
                    except TypeError: continue
                    meta = obj._meta
                    if meta.object_name in sys.get_keyspace_column_families(params['KEYSPACE']).keys():
                        msg = ''.join((
                        'Looks like you already have a `', meta.object_name,
                        '` column family in keyspace `', params['KEYSPACE'],
                        '`. Do you want to delete and recreate it? ',
                        'All current data will be deleted! (y/n): ',
                        ))
                        resp = raw_input(msg)
                        if not resp or resp[0] != 'y':
                            print "Ok, then we're done here."
                            return
                        sys.drop_column_family(params['KEYSPACE'], meta.object_name)
                    sys.create_column_family(
                        keyspace = params['KEYSPACE'],
                        name = meta.object_name,
                        super = meta.super_cf,
                        comparator_type = meta.comparator_type,
                        subcomparator_type = meta.subcomparator_type if meta.super_cf else None,
                        default_validation_class = meta.default_validation_class,
                        key_validation_class = meta.key_validation_class,
                        column_validation_classes = meta.column_validators,
                        comment = meta.comment,
                        key_cache_size = meta.key_cache_size,
                        gc_grace_seconds = meta.gc_grace_seconds,
                        min_compaction_threshold = meta.min_compaction_threshold,
                        max_compaction_threshold = meta.max_compaction_threshold,
                        key_cache_save_period_in_seconds = meta.key_cache_save_period_in_seconds,
                        row_cache_save_period_in_seconds = meta.row_cache_save_period_in_seconds,
                        replicate_on_write = meta.replicate_on_write,
                        merge_shards_chance = meta.merge_shards_chance,
                        row_cache_provider = meta.row_cache_provider,
                        key_alias = meta.key_alias,
                        compaction_strategy = meta.compaction_strategy,
                        compaction_strategy_options = meta.compaction_strategy_options,
                        compression_options = meta.compression_options,
                        row_cache_keys_to_save = meta.row_cache_keys_to_save,
                    )

        print 'All done!'
