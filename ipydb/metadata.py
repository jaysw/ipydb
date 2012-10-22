# -*- coding: utf-8 -*-

"""
Reading and caching command-line completion strings from 
a database schema. 

:copyright: (c) 2012 by Jay Sweeney.
:license: see LICENSE for more details.
"""

import os
import multiprocessing
from multiprocessing.pool import ThreadPool
from collections import defaultdict
import sqlite3
import datetime
from datetime import timedelta
from dateutil import parser
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
from IPython.utils.path import locate_profile

CACHE_MAX_AGE = 60 * 5 # invalidate connection metadata if it is older than CACHE_MAX_AGE

class CompletionDataAccessor(object):
    '''reads and writes db-completion data from/to an sqlite db'''
    pool = ThreadPool(multiprocessing.cpu_count() * 2)

    def __init__(self):
        self.metadata = defaultdict(self._meta)
        self.dbfile = os.path.join(locate_profile(), 'ipydb.sqlite')
        self.dburl = 'sqlite:////%s' % self.dbfile
        self.db = sa.engine.create_engine(self.dburl)
        self.create_schema()
        self._sa_metadata = None

    def _meta(self):
        return {
            'tables': set(),
            'fields': set(),
            'dottedfields': set(),
            'types' : dict(),
            'isempty' : True,
            'reflecting': False,
            'created': datetime.datetime(datetime.MINYEAR, 1, 1)
        }

    def sa_metadata():
        def fget(self):
            meta = getattr(self, '_sa_metadata', None)
            if meta is None:
                self._sa_metadata = sa.MetaData()
            return self._sa_metadata
        return locals()
    sa_metadata = property(**sa_metadata())
    
    def get_metadata(self, db, noisy=True):
        db_key = self.get_db_key(db.url)
        metadata = self.metadata[db_key]
        if metadata['isempty']: # XXX: what if the DB schema exists, but is empty?!
            self.read(db_key) # XXX is this slow? perhaps use self.pool.apply_async 
        now = datetime.datetime.now()
        if (metadata['isempty'] or (now - metadata['created']) > 
                                    timedelta(seconds=CACHE_MAX_AGE)) \
                and not metadata['reflecting']:
            if noisy:
                print "Reflecting metadata..."
            metadata['reflecting'] = True
            def printtime(x):
                pass
                #print "completed in %.2s" % (time.time() - t0)
            self.pool.apply_async(self.reflect_metadata, (db,), callback=printtime)
        return metadata

    def reflect_metadata(self, target_db):
        db_key = self.get_db_key(target_db.url)
        table_names = target_db.table_names()
        self.pool.map(self.reflect_table, ( (target_db, db_key, tablename) for tablename in sorted(table_names) ))
        self.metadata[db_key]['created'] = datetime.datetime.now()
        self.metadata[db_key]['reflecting'] = False

        # write to database. 
        self.write_all(db_key)

    def reflect_table(self, arg):
        target_db, db_key, tablename = arg # XXX: this sux
        metadata = self.sa_metadata # XXX: not threadsafe
        self.sa_metadata.bind = target_db
        t = sa.Table(tablename, metadata, autoload=True)
        tablename = t.name.lower()
        self.metadata[db_key]['tables'].add(tablename)
        self.metadata[db_key]['isempty'] = False
        for col in t.columns:
            fieldname = col.name.lower()
            dottedname = tablename + '.' + fieldname
            self.metadata[db_key]['fields'].add(fieldname)
            self.metadata[db_key]['dottedfields'].add(dottedname)
            self.metadata[db_key]['types'][dottedname] = str(col.type)
        
    def get_db_key(self, url):
        '''minimal unique key for describing a db connection'''
        return str(URL(url.drivername, url.username, host=url.host,
                   port=url.port, database=url.database))

    def read(self, db_key):
        with sqlite3.connect(self.dbfile) as db:
            result = db.execute("""
                select 
                    t.db_key,
                    t.name as tablename, 
                    f.name as fieldname,
                    f.type as type
                from dbtable t inner join dbfield f 
                    on f.table_id = t.id
                where
                    t.db_key = :db_key
            """, dict(db_key=db_key))
            for r in result:
                self.metadata[db_key]['isempty'] = False
                self.metadata[db_key]['tables'].add(r[1])
                self.metadata[db_key]['fields'].add(r[2])
                self.metadata[db_key]['dottedfields'].add(
                        '%s.%s' % (r[1], r[2]))
                self.metadata[db_key]['types']['%s.%s' % (r[1], r[2])] = r[3]

            result = db.execute("select max(created) as created from dbtable " \
                    "where db_key = :db_key", dict(db_key=db_key)).fetchone()
            if result[0]:
                self.metadata[db_key]['created'] = parser.parse(result[0])
            else:
                self.metadata[db_key]['created'] = datetime.datetime.now()

    def create_schema(self):
        meta = sa.MetaData()
        meta.reflect(bind=self.db)
        if 'dbtable' not in meta.tables:
            self.db.execute("""
                create table dbtable (
                    id integer primary key,
                    db_key text not null, 
                    name text not null,
                    created datetime not null default current_timestamp,
                    constraint db_table_unique
                        unique (db_key, name) 
                        on conflict rollback
                )
            """)
        if 'dbfield' not in meta.tables:
            self.db.execute("""
                create table dbfield (
                    id integer primary key,
                    table_id integer not null
                        references dbtable(id)
                        on delete cascade
                        on update cascade,
                    name text not null,
                    type text,
                    constraint db_field_unique
                        unique (table_id, name) 
                        on conflict rollback
                )
            """)

    def flush(self):
        self.pool.terminate()
        self.pool.join()
        self.metadata = defaultdict(self._meta)
        self.delete_schema()
        self.create_schema()
        self.pool = ThreadPool(multiprocessing.cpu_count() * 2)


    def delete_schema(self):
        self.db.execute("""drop table dbfield""")
        self.db.execute("""drop table dbtable""")

    def tables(self, db):
        db_key = self.get_db_key(db.url)
        return self.metadata[db_key]['tables']

    def fields(self, db):
        db_key = self.get_db_key(db.url)
        return self.metadata[db_key]['fields']

    def dottedfields(self, db):
        db_key = self.get_db_key(db.url)
        return self.metadata[db_key]['dottedfields']

    def types(self, db):
        return self.metadata[self.get_db_key(db.url)]['types']

    def write_all(self, db_key):
        with sqlite3.connect(self.dbfile) as sqconn:
            for dottedname in self.metadata[db_key]['dottedfields']:
                tablename, fieldname = dottedname.split('.', 1)
                type_ = self.metadata[db_key]['types'].get(dottedname, '')
                self.write(sqconn, db_key, tablename, fieldname, type_) # might already be thread safe?

    def write(self, sqconn, db_key, table, field, type_=''):
        res = sqconn.execute(
            "select id from dbtable where db_key=:db_key and name=:table",
            dict(db_key=db_key, table=table))
        table_id = None
        row = res.fetchone()
        if row is not None:
            table_id = row[0]
        else:
            res = sqconn.execute("""
                insert into dbtable(db_key, name) values (
                    :db_key, :table)""",
                dict(db_key=db_key, table=table))
            table_id = res.lastrowid
        try:
            sqconn.execute("""
                insert into dbfield(table_id, name, type) values (
                    :table_id, :field, :type)""",
                dict(table_id=table_id, field=field, type=type_))
        except sqlite3.IntegrityError: # exists
            sqconn.execute("""
                update dbfield set 
                    type = :type
                where
                    table_id = :table_id
                    and name = :field""",
                dict(table_id=table_id, field=field, type=type_))

