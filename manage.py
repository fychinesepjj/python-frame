#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import logging
import time
from datetime import datetime
from core.db import mysql
from core.application import WSGIApplication, Jinja2TemplateEngine
from settings.config import configs

logging.basicConfig(level=logging.INFO)
root_dir = os.path.realpath(os.path.dirname(__file__))
sys.path.append(root_dir)


def datetime_filter(t):
    delta = int(time.time() - t)
    if delta < 60:
        return u'Less Than 1 Min'
    if delta < 3600:
        return u'Less Than %s Mins' % (delta // 60)
    if delta < 86400:
        return u'Less Than %s Hours' % (delta // 3600)
    if delta < 604800:
        return u'Less Than %s Days' % (delta // 86400)
    dt = datetime.fromtimestamp(t)
    return u'%s-%s-%s' % (dt.year, dt.month, dt.day)

mysql.create_engine(**configs.db)
wsgi = WSGIApplication(root_dir)
template_engine = Jinja2TemplateEngine(os.path.join(root_dir, 'templates'))
template_engine.add_filter('datetime', datetime_filter)
wsgi.template_engine = template_engine

import urls
wsgi.add_module(urls)

if __name__ == '__main__':
    wsgi.run(9000, host='0.0.0.0')
else:
    application = wsgi.get_wsgi_application()







