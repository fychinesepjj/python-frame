# -*- coding: utf-8 -*-
import db
from db import next_id
from orm import Model, StringField, IntegerField


class Test(Model):
    __table__ = 'test'
    id = StringField(primary_key=True, default=next_id, ddl='varchar(50)')
    name = StringField(ddl='varchar(50)')
    age = IntegerField(ddl='int')

if __name__ == '__main__':
    db.create_engine('mysql', 'root', '123456', 'frame-test')
    t = Test.find_by('where age > ?', 0)
    print t
