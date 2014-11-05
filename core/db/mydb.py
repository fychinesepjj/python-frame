#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import uuid
import functools
import threading
import logging


class DBError(Exception):
    pass


class MultiColumnsError(DBError):
    pass


class Dict(dict):
    '''
    Simple dict but support access by x.y style
    >>> d1 = Dict()
    >>> d1['x'] = 100
    >>> d1.x
    100
    >>> d2 = Dict(a=1, b=2, c=3)
    >>> d2.c
    3
    >>> d2['empty']
    Traceback (most recent call last):
        ...
    KeyError: 'empty'
    '''

    def __init__(self, names=(), values=(), **kwargs):
        super(Dict, self).__init__(**kwargs)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"Dict object has no attribute '%s'" % key)


def next_id(t=None):
    '''
    Return next id as 50-char string

    Args:
        t: unix timestamp, default to None and using time.time()
    '''
    if t is None:
        t = time.time()
    return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)


def _profiling(start, sql=''):
    t = time.time() - start
    if t > 0.1:
        logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))
    else:
        logging.info('[PROFILING] [DB] %s: %s' % (t, sql))


class _LazyConnection(object):
    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            connection = engine.connect()
            logging.info('open connection <%s>...' % hex(id(connection)))
            self.connection = connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            connection = self.connection
            self.connection = None
            logging.info('close connection <%s>...' % hex(id(connection)))
            connection.close()


class _DbCtx(threading.local):
    '''
    Thread local object that holds connection info
    '''
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        logging.info('open lazy connection...')
        self.connection = _LazyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()

#thread-local db context
_db_ctx = _DbCtx()

#global engine object
engine = None


class _Engine(object):
    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()


class _ConnectionCtx(object):
    '''
    _ConnectionCtx object than can open and close connection context.
    _ConnectionCtx object can be nested and only the most outer connection has effect
    with connection():
        pass
        with connection():
            pass
    '''
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()


def connection():
    '''
    Return _ConnectionCtx object that can be used by 'with' statement
    with connection():
        pass
    '''
    return _ConnectionCtx()


def with_connection(func):
    '''
    Decorator for reuse connection
    @with_connection
    def foo(*args, **kwargs):
        f1()
        f2()
    '''
    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        with _ConnectionCtx():
            return func(*args, **kwargs)
    return _wrapper


def mysql_engine(user, password, database, host='127.0.0.1', port=3306, **kwargs):
    import MySQLdb
    params = dict(user=user, passwd=password, db=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8', connect_timeout=10)
    for k, v in defaults.iteritems():
        params[k] = kwargs.pop(k, v)
    params.update(kwargs)
    return lambda: MySQLdb.connect(**params)


def create_engine(detail_engine, user, password, database, host='127.0.0.1', port=3306, **kwargs):
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')
    connector = detail_engine(user, password, database, host, port, **kwargs)
    engine = _Engine(connector)
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))


if __name__ == '__main__':
    #import doctest
    #doctest.testmod()
    create_engine(mysql_engine, 'root', '123456', 'frame-test')
    conn = engine.connect()
    cursor = conn.cursor()
