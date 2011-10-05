from redis_wrap import *

GLOBAL_LINK = "__aliastable__" #Stores links between different models

def create_link(from_key, to_key):
    """
    Creates a symlink between two keys when using the models API.
    Note: This requires the subclassing of Model, a simple get_redis() call won't work as intended.
    """
    table = get_hash(GLOBAL_LINK)
    table[from_key] = to_key



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
                delattr(cls, k)

        cls._ctr = get_redis().get('%s.counter' % cls.__name__)

    def create(cls):
        cls._ctr = get_redis().incr('%s.counter' % cls.__name__)
        return cls(cls._ctr)

class SortedSet(object):
    ''' Wrapper for redis sorted set '''
    def  __init__(self, key):
        self.key = key

    def remove(self, field):
        get_redis().zrem(self.key, field)

    def push(self, field, score):
        get_redis().zincrby(self.key, field, score)

    def pull(self, limit = 10):
        return get_redis().zrevrange(self.key, 0, limit - 1) or []



class Model(object):
    def __init__(self, prefix):
        self.prefix = '%s.%s' % (self.__class__.__name__, prefix)
        self.id = prefix
        self.check_links()

    def check_links(self):
        #Check for symlinks
        if self.prefix in get_hash(GLOBAL_LINK):
            target_key = get_hash(GLOBAL_LINK)[self.prefix] or ''
            if target_key and get_redis().exists(target_key):
                self.prefix = target_key
                self.id = target_key.split('.', 1)[1]


    def __getattr__(self, key):
        if key in ['prefix', 'id']:
            return super(Model, self).__getattribute__(key)
        else:
            _key = "%s.%s" % (self.prefix, key)
            if key in self._sets:
                return get_set(_key)
            elif key in self._lists:
                return get_list(_key)
            elif key in self._zsets:
                return SortedSet(_key)
            elif key in self._hashes:
                return get_hash(_key)
            return get_redis().get(_key) or ''

    def __setattr__(self, name, value):
        if name in ['prefix', 'id']:
            super(Model, self).__setattr__(name, value)
        else:
            get_redis().set("%s.%s" % (self.prefix, name), value)

    def rel(self, key):
        self.check_links()
        return "%s.%s" % (self.prefix, key)

    def obj(self):
        '''returns a hash object'''
        return get_hash(self.rel('info'))

    #Access properties from .info hash
    def get(self, key):
        info = get_hash(self.rel('info'))
        return info.get(key, '')

    def set(self, key, value):
        info = get_hash(self.rel('info'))
        info[key] = value

    def delete(self, key):
        info = get_hash(self.rel('info'))
        del info[key]

    def get_permalink(self):
        return self.__class__.__name__.lower() + '/' + self.id + '/'

    def exists(self):
        return get_redis().exists(self.prefix)

    __metaclass__ = Meta


def copy_keys(wrapper, dic):
    ''' Copies keys from a python dict to a Redis Hash wrapper '''
    for k, v in dic.items():
        wrapper[k] = v
