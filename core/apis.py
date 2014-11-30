#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re, json, logging, functools
from application import ctx


def dumps(obj):
    return json.dumps(obj)


class APIError(StandardError):
    def __init__(self, error, data='', message=''):
        super(APIError, self).__init__(message)
        self.error = error
        self.data = data
        self.message = message


class APIValueError(APIError):
    def __init__(self, field, message=''):
        super(APIValueError, self).__init__('value:invalid', field, message)


class APIResourceNotFoundError(APIError):
    def __init__(self, field, message=''):
        super(APIResourceNotFoundError, self).__init__('value:notfound', field, message)


class APIPermissionError(APIError):
    def __init__(self, message=''):
        super(APIPermissionError, self).__init__('permission:forbidden', 'permission', message)


def api(func):
    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        try:
            r = dumps(func(*args, **kwargs))
        except APIError, e:
            r = json.dumps(dict(error=e.error, data=e.data, message=e.message))
        except Exception, e:
            logging.exeception(e)
            r = json.dumps(dict(error='internalerror', data=e.__class__, message=e.message))
        ctx.response.content_type = 'application/json'
        return r
    return _wrapper
