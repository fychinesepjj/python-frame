# -*- coding: utf-8 -*-
import time
import uuid
import functools
import threading
import logging
from core.utils import Dict


class DBError(Exception):
    pass


class MultiColumnsError(DBError):
    pass


class ColumnTypeError(DBError):
    pass


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


class _TransactionCtx(object):
    '''
    _TransactionCtx object that can handle transactions
    with _TransactionCtx():
        pass
    '''
    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.Init()
            self.should_close_conn = True
        _db_ctx.transactions = _db_ctx.transactions + 1
        logging.info('begin transaction...' if _db_ctx.transactions == 1 else 'join current transaction...')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1
        try:
            if _db_ctx.transactions == 0:
                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

        def commit(self):
            global _db_ctx
            logging.info('commit transaction...')
            try:
                _db_ctx.connection.commit()
                logging.info('commit ok.')
            except:
                logging.warning('commit failed. try rollback...')
                _db_ctx.connection.rollback()
                logging.warning('rollback ok.')
                raise

        def rollback(self):
            global _db_ctx
            logging.warning('rollback transaction...')
            _db_ctx.connection.rollback()
            logging.info('rollback ok.')


def transaction():
    '''
    Create a transaction object, so can use with statement:
    with transaction():
        pass
    '''
    return _TransactionCtx()


def with_transaction(func):
    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        _start = time.time()
        with _TransactionCtx():
            return func(*args, **kwargs)
        _profiling(_start)
    return _wrapper


def _select(sql, first, *args):
    'execute select SQL and return unique result or list results'
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        #if select statement, cursor have description attr, or description will be None
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()


@with_connection
def select_one(sql, *args):
    return _select(sql, True, *args)


@with_connection
def select_int(sql, *args):
    d = _select(sql, True, *args)
    if len(d) != 1:
        raise MultiColumnsError('Expect only one column')
    v = d.values()[0]
    if not isinstance(v, int):
        raise ColumnTypeError('Expect only integer column')
    return d.values()[0]


@with_connection
def select(sql, *args):
    return _select(sql, False, *args)


@with_connection
def _update(sql, *args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
            cursor = _db_ctx.connection.cursor()
            cursor.execute(sql, args)
            r = cursor.rowcount
            if _db_ctx.transaction == 0:
                logging.info('auto commit')
                _db_ctx.connection.commit()
            return r
    finally:
        if cursor:
            cursor.close()


def insert(table, **kwargs):
    cols, args = zip(*kwargs.iteritems())
    sql = 'insert into `%s` (%s) values (%s)' % (table, ','.join(['`%s`' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
    return _update(sql, *args)


def update(sql, *args):
    return _update(sql, *args)


def mysql_engine(user, password, database, host='127.0.0.1', port=3306, **kwargs):
    import MySQLdb
    params = dict(user=user, passwd=password, db=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8', connect_timeout=10)
    for k, v in defaults.iteritems():
        params[k] = kwargs.pop(k, v)
    params.update(kwargs)
    return lambda: MySQLdb.connect(**params)


def select_engine(name='mysql'):
    mapping = {
        'mysql': mysql_engine,
    }
    if name in mapping:
        return mapping[name]


def create_engine(engine_name, user, password, database, host='127.0.0.1', port=3306, **kwargs):
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')
    detail_engine = select_engine(engine_name)
    if detail_engine is None:
        raise DBError('Engine is not supported, %s' % engine_name)
    connector = detail_engine(user, password, database, host, port, **kwargs)
    engine = _Engine(connector)
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))


if __name__ == '__main__':
    #import doctest
    #doctest.testmod()
    create_engine('mysql', 'root', '123456', 'frame-test')
    print select_int('select name from test')
