#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os, re, time, base64, hashlib, logging
from core.application import get, post, ctx, view, interceptor, seeother, notfound
from models import User, Blog, Comment


@view('blogs.html')
@get('/')
def index():
    blogs = Blog.find_all()
    user = User.find_first('where email=?', 'admin@admin.com')
    return dict(blog=blogs, user=user)
