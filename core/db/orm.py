# -*- coding: utf-8 -*-
import logging
import db


class Field(object):
    _count = 0

    def __init__(self, **kwargs):
        self.name = kwargs.get('name', None)
        self._default = kwargs.get('default', None)
        self.primary_key = kwargs.get('primary_key', False)
        self.nullable = kwargs.get('nullable', False)
        self.insertable = kwargs.get('insertable', True)
        self.updatable = kwargs.get('updatable', True)
        self.ddl = kwargs.get('ddl', '')
        self.order = Field._count
        Field._count = Field._count + 1

    @property
    def default(self):
        d = self._default
        return d() if callable(d) else d

    def __str__(self):
        s = ['<%s:%s,%s,default(%s),' % (self.__class__.__name__, self.name, self.ddl, self._default)]
        self.nullable and s.append('N')
        self.updatable and s.append('U')
        self.insertable and s.append('I')
        s.append('>')
        return ''.join(s)

    def __unicode__(self):
        return self.__str__()


class StringField(Field):
    def __init__(self, **kwargs):
        if not 'default' in kwargs:
            kwargs['default'] = ''
        if not 'ddl' in kwargs:
            kwargs['ddl'] = 'varchar(255)'
        super(StringField, self).__init__(**kwargs)


class IntegerField(Field):
    def __init__(self, **kwargs):
        if not 'default' in kwargs:
            kwargs['default'] = 0
        if not 'ddl' in kwargs:
            kwargs['ddl'] = 'bigint'
        super(IntegerField, self).__init__(**kwargs)


class FloatField(Field):
    def __init__(self, **kwargs):
        if not 'default' in kwargs:
            kwargs['default'] = 0.0
        if not 'ddl' in kwargs:
            kwargs['ddl'] = 'real'
        super(FloatField, self).__init__(**kwargs)


class BoolField(Field):
    def __init__(self, **kwargs):
        if not 'default' in kwargs:
            kwargs['default'] = False
        if not 'ddl' in kwargs:
            kwargs['ddl'] = 'bool'
        super(BoolField, self).__init__(**kwargs)


class TextField(Field):
    def __init__(self, **kwargs):
        if not 'default' in kwargs:
            kwargs['default'] = ''
        if not 'ddl' in kwargs:
            kwargs['ddl'] = 'text'
        super(TextField, self).__init__(**kwargs)


class BlobField(Field):
    def __init__(self, **kwargs):
        if not 'default' in kwargs:
            kwargs['default'] = ''
        if not 'ddl' in kwargs:
            kwargs['ddl'] = 'blob'
        super(BlobField, self).__init__(**kwargs)


class VersionField(Field):
    def __init__(self, name=None):
        super(VersionField, self).__init__(name=name, default=0, ddl='int')

_triggers = frozenset(['pre_insert', 'pre_update', 'pre_delete'])


def _gen_sql(table_name, mapping):
    pk = None
    sql = ['-- generating SQL for %s:' % table_name, 'create table `%s` (' % table_name]
    for f in sorted(mapping.values, lambda x, y: cmp(x._order, y._order)):
        if not hasattr(f, 'ddl'):
            raise StandardError('no ddl in field "%s".' % f)
        ddl = f.ddl
        nullable = f.nullable
        if f.primary_key:
            pk = f.name
        sql.append(nullable and '  `%s` %s,' % (f.name, ddl) or '  `%s` %s not null,' % (f.name, ddl))
        sql.append('  primary key(`%s`)' % pk)
        sql.append(');')
        return '\n'.join(sql)


class ModelMetaClass(type):
    '''
    MetaClass for model objects
    '''
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        if not name in cls.subclasses:
            cls.subclasses[name] = name
        else:
            logging.warning('Redefine class: %s' % name)

        logging.info('Scan ORMapping %s...' % name)
        mapping = dict()
        primary_key = None
        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                if not v.name:
                    v.name = k
                    logging.info('Found mapping: %s => %s' % (k, v))
                    #check duplicate primary key
                    if v.primary_key:
                        if primary_key:
                            raise TypeError('Cannot define more than 1 primary key in class: %s' % name)
                        if v.updatable:
                            logging.warning('NOTE: change primary key <%s> to non-updatable.' % k)
                            v.updatable = False
                        if v.nullable:
                            logging.warning('NOTE: change primary key <%s> to non-nullable.' % k)
                            v.nullable = False
                        primary_key = v
                    mapping[k] = v
        #check exist of primary key
        if not primary_key:
            raise TypeError('Primary key not defined in class: %s' % name)
        for k in mapping.iterkeys():
            attrs.pop(k)

        if not '__table__' in attrs:
            attrs['__table__'] = name.lower()
        attrs['__mapping__'] = mapping
        attrs['__primary_key__'] = primary_key
        attrs['__sql__'] = lambda self: _gen_sql(attrs['__table__'], mapping)
        for trigger in _triggers:
            if not trigger in attrs:
                attrs['trigger'] = None
        return type.__new__(cls, name, bases, attrs)


class Model(dict):
    __metaclass__ = ModelMetaClass

    def __init__(self, **kwargs):
        super(Model, self).__init__(**kwargs)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    @classmethod
    def get(cls, pk):
        '''
        Get by primary key
        '''
        d = db.select_one('select * from %s where %s=?' % (cls.__table__, cls.__primary_key__.name), pk)
        return cls(**d) if d else None

    @classmethod
    def find_one(cls, where, *args):
        '''
        Find by where clause and return one result. If multiple results found,
        only the first one returned. If no results found, return None
        '''
        d = db.select_one('select * from `%s` %s' % (cls.__table__, where), *args)
        return cls(**d) if d else None

    @classmethod
    def find_all(cls, *args):
        '''
        Find all and return list.
        '''
        L = db.select('select * from `%s`' % cls.__table__)
        return [cls(**d) for d in L]

    @classmethod
    def find_by(cls, where, *args):
        '''
        Find by where clause and return list
        '''
        L = db.select('select * from `%s` %s' % (cls.__table__, where), *args)
        return [cls(**d) for d in L]

    @classmethod
    def count_all(cls):
        '''
        Find by 'select count(pk) from table' and return integer.
        '''
        return db.select_int('select count(`%s`) from `%s`' % (cls.__primary_key__.name, cls.__table__))

    @classmethod
    def count_by(cls, where, *args):
        '''
        Find by 'select count(pk) from table where...' and return int
        '''
        return db.select_int('select count(`%s`) from `$s` %s' % (cls.__primary_key__.name, cls.__table__, where), *args)

    def update(self):
        self.pre_update and self.pre_update()
        L = []
        args = []
        for k, v in self.__mapping__.iteritems():
            if v.updatable:
                if hasattr(self, k):
                    arg = getattr(self, k)
                else:
                    arg = v.default
                    setattr(self, k, arg)
                L.append('`%s`=?' % k)
                args.append(arg)
        pk = self.__primary_key__.name
        args.append(getattr(self, pk))
        db.update('update `%s` set %s where %s=?' % (self.__table__, ','.join(L), pk), *args)
        return self

    def delete(self):
        self.pre_delete and self.pre_delete()
        pk = self.__primary_key__.name
        args = (getattr(self, pk), )
        db.update('delete from `%s` where `%s`=?' % (self.__table__, pk), *args)
        return self

    def insert(self):
        self.pre_insert and self.pre_insert()
        params = {}
        for k, v in self.__mapping__.iteritems():
            if v.insertable:
                if not hasattr(self, k):
                    setattr(self, k, v.default)
                params[v.name] = getattr(self, k)
        db.insert('%s' % self.__table__, **params)
        return self


if __name__ == '__main__':
    f = Field(name='abc')
    print f
