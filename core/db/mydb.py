#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import uuid
import functools
import threading
import logging


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


if __name__ == '__main__':
    import doctest
    doctest.testmod()
