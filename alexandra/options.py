from django.db.models.options import get_verbose_name
from django.conf import settings
from alexandra.exceptions import ConfigurationError
from alexandra.manager import Manager
from alexandra import types

DEFAULT_NAMES = ('verbose_name', 'app_label', 'pool_name')
TRANSIT_NAMES = ('read_consistency_level', 'write_consistency_level',
                'key_validation_class', 'default_validation_class',
                'column_validators', 'comparator_type',
                'subcomparator_type', 'super_cf', 'comment',
                'key_cache_size', 'row_cache_size',
                'gc_grace_seconds', 'read_repair_chance',
                'min_compaction_threshold', 'max_compaction_threshold',
                'key_cache_save_period_in_seconds',
                'row_cache_save_period_in_seconds',
                'replicate_on_write', 'merge_shards_chance',
                'row_cache_provider', 'key_alias',
                'compaction_strategy', 'compaction_strategy_options',
                'row_cache_keys_to_save', 'compression_options',
                )

class Options(object):

    def __init__(self, meta, pool_name='default', app_label=None):

        self.meta = meta
        self.app_label = app_label
        self.object_name = None
        self.verbose_name = None
        self.pk = None
        for name in TRANSIT_NAMES:
            setattr(self, name, None)
        self.pool_name = pool_name
        self.super_cf = False
        self.comparator_type = types.UTF8Type()

    def apply_names(self, to):
        for name in TRANSIT_NAMES:
            value = getattr(self, name)
            if value is not None:
                setattr(to, name, value)

    def contribute_to_class(self, cls, name):
        cls._meta = self
        self.object_name = cls.__name__
        self.verbose_name = get_verbose_name(self.object_name)
        if self.meta:
            if hasattr(self.meta, 'cf'):
                self.object_name = self.meta.cf
            meta_attrs = self.meta.__dict__.copy()
            for name in self.meta.__dict__:
                if name.startswith('_'):
                    del meta_attrs[name]
            for attr_name in DEFAULT_NAMES + TRANSIT_NAMES:
                if attr_name in meta_attrs:
                    setattr(self, attr_name, meta_attrs.pop(attr_name))
                elif hasattr(self.meta, attr_name):
                    setattr(self, attr_name, getattr(self.meta, attr_name))

        if getattr(settings, 'RUNNING_TESTS', False):
            self.keyspace = 'test_%s' % self.keyspace
        cls.add_to_class('objects', Manager())        

        del self.meta

