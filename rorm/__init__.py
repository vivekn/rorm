from redis_wrap import *

class Field(object):
    def __init__(self, data_type)
        self.data_type = data_type

class Meta(type):
    def __init__(cls, name, bases, ns):
        cls._hashes = []
        cls._sets = []
        cls._lists = []

        for k,v in ns.items():
            if isinstance(v, Field):
                if v.data_type == 'set':
                    cls._sets.append(k)
                elif v.data_type == 'list':
                    cls._lists.append(k)
                elif v.data_type == 'hash':
                    cls._hashes.append(k)

class Model(object):

    def __init__(self, prefix):
        self.prefix = prefix

    def __getattr__(self, key):
        if (hasattr(self, key)):
            super(Model, self).__getattr__(key) 
        _key = "%s.%s" % (self.prefix, key)
        if key in self._sets:
            return get_set(_key)
        elif key in self._lists:
            return get_list(_key)
        elif key in self._hashes:
            return get_hash(_key)
        return key

    def get(self, key):
        _key = "%s.%s" % (self.prefix, key)
        return get_redis.get(_key)

    def rel(self, key):
        return self.__getattr__(key)





                                    
