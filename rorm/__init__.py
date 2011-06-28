from redis_wrap import *

class Field(object):
    def __init__(self, data_type):
        self.data_type = data_type

class Meta(type):
    def __init__(cls, name, bases, ns):
        cls._hashes = []
        cls._sets = []
        cls._lists = []
        cls._zsets = []

        for k,v in ns.items():
            if isinstance(v, Field):
                if v.data_type == 'set':
                    cls._sets.append(k)
                elif v.data_type == 'list':
                    cls._lists.append(k)
                elif v.data_type == 'hash':
                    cls._hashes.append(k)
                elif v.data_type == 'zset':
                    cls._zsets.append(k)
                del ns[k]
                

        cls._ctr = get_redis.get('%s.counter' % cls.__name__)

    def create(cls):
        cls._ctr = get_redis().incr('%s.counter' % cls.__name__)
        return cls(cls._ctr)

class SortedSet(object):
    ''' Wrapper for redis sorted set '''
    def  __init__(self, key):
        self.key = key

    def remove(self, field):
        get_redis().zrem(self.key, field)

    def push(self, field, score)
        get_redis().zincrby(self.key, score, field)

    def pull(self, limit = 10)
        get_redis().zrevrange(self.key, 0, limit - 1)



class Model(object):
    def __init__(self, prefix):
        self.prefix = '%s.%s' % (self.__class__.__name__, prefix)
        self.id = prefix

    def __getattr__(self, key):
        _key = "%s.%s" % (self.prefix, key)
        if key in self._sets:
            return get_set(_key)
        elif key in self._lists:
            return get_list(_key)
        elif key in self._zsets:
            return get_list(_key)
        elif key in self._hashes:
            return get_hash(_key)
        return _key

    def get(self, key):
        key = "%s.%s" % (self.prefix, key)
        return get_redis.get(_key)

    def rel(self, key):
        return "%s.%s" % (self.prefix, key)

    def obj(self):
        '''returns a hash object'''
        return get_hash(self.info)


    __metaclass__ = Meta


def copy_keys(wrapper, dic):
    ''' Copies keys from a python dict to a Redis Hash wrapper '''
    for k, v in dic.items():
        wrapper[k] = v
