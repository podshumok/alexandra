#import pycassa
import alexandra
from django.conf import settings
from django.db.models.manager import ManagerDescriptor

from alexandra import pools

class MetaManager(type):
    def __init__(cls, name, bases, attrs):
        if settings.DEBUG:
            import inspect
            from alexandra.logger import logged_func

            for base in bases:
                for attr in dir(base):
                    value = getattr(base, attr)
                    if not attr.startswith('_') and inspect.ismethod(value):
                        setattr(cls, attr, logged_func(value))
            
            for attr, value in attrs.items():
                if not attr.startswith('_') and inspect.isfunction(value):
                    setattr(cls, attr, logged_func(value))

class AlexandraManagerDescriptor(ManagerDescriptor):
    def __init__(self, manager, *args, **kwargs):
        super(AlexandraManagerDescriptor, self).__init__(manager, *args, **kwargs)
        self._manager = manager

    def __get__(self, instance, *args, **kwargs):
        if not self._manager._ready: self._manager._init()
        return super(AlexandraManagerDescriptor, self).__get__(instance, *args, **kwargs)

class Manager(alexandra.CF):
    
    __metaclass__ = MetaManager
    
    def __init__(self):
        self.model = None
    
    def contribute_to_class(self, model, name,):
        self.model = model
        setattr(model, name, AlexandraManagerDescriptor(self))
        meta = model._meta
        self.pool_name = meta.pool_name if meta.pool_name else 'default'
        self._ready = False

    def _init(self):
        model = self.model
        meta = model._meta
        pool = pools[self.pool_name]
        if pool is None:
            params = settings.CASSANDRA[self.pool_name]
            adv = params.get('PARAMS', {})
            pools[self.pool_name] = alexandra.Pool(params['KEYSPACE'], params['CLUSTER'], **adv)
            pool = pools[self.pool_name]
        super(Manager, self).__init__(pools[self.pool_name],
            meta.object_name,
            dict_class=model, super=meta.super_cf)
        meta.apply_names(self)
        self._ready = True

    def get(self, key, **kwargs):
        if 'column_count' not in kwargs:
            kwargs['column_count'] = 1000000
        obj = super(Manager, self).get(key, **kwargs)
        obj.pk = key
        return obj
    
    def multiget(self, keys, **kwargs):
        if 'column_count' not in kwargs:
            kwargs['column_count'] = 1000000
        objs = super(Manager, self).multiget(keys, **kwargs)
        for k, v in objs.iteritems():
            v.pk = k
        return objs
    
    def insert(self, key, columns, write_consistency_level=None):
        obj = self.model(columns)
        obj.pk = key
        return obj.save(write_consistency_level=write_consistency_level)
    
    def _insert(self, key, columns, write_consistency_level=None):
        for k,v in columns.iteritems():
            if isinstance(v, dict):
                for subk, subv in v.iteritems():
                    columns[k][subk] = (subv)
            else:
                columns[k] = (v)
        return super(Manager, self).insert(key, columns, write_consistency_level=write_consistency_level)
        
    def _remove(self, key, column=None, write_consistency_level = None):
        return super(Manager, self).remove(key, column=column, write_consistency_level=write_consistency_level)
