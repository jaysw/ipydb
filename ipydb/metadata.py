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
        self.create_schema()

    def _meta(self):
        return {
            'tables': set(),
            'fields': set(),
            'dottedfields': set(),
            'isempty' : True,
            'reflecting': False,
            'created': datetime.datetime(datetime.MINYEAR, 1, 1)
        }
    
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

        # write out with a single thread:
        with sqlite3.connect(self.dbfile) as sqconn:
            for tablename, fieldname in ( f.split('.') for f in self.metadata[db_key]['dottedfields'] ):
                self.write(sqconn, db_key, tablename, fieldname) # might already be thread safe?

    def reflect_table(self, arg):
        target_db, db_key, tablename = arg # XXX: this sux
        metadata = sa.MetaData(bind=target_db)
        t = sa.Table(tablename, metadata, autoload=True)
        fieldnames = map(unicode.lower, t.columns.keys())
        dotted = map(lambda fieldname: tablename + '.' + fieldname, fieldnames)
        # synchronise self.metadata !!
        self.metadata[db_key]['tables'].add(tablename)
        self.metadata[db_key]['fields'].update(fieldnames)
        self.metadata[db_key]['dottedfields'].update(dotted)
        self.metadata[db_key]['isempty'] = False

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
                    f.name as fieldname
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

            result = db.execute("select max(created) as created from dbtable " \
                    "where db_key = :db_key", dict(db_key=db_key)).fetchone()
            if result[0]:
                self.metadata[db_key]['created'] = parser.parse(result[0])
            else:
                self.metadata[db_key]['created'] = datetime.datetime.now()

    def create_schema(self):
        db = sa.engine.create_engine(self.dburl)
        meta = sa.MetaData()
        meta.reflect(bind=db)
        if 'dbtable' not in meta.tables:
            db.execute("""
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
            db.execute("""
                create table dbfield (
                    id integer primary key,
                    table_id integer not null
                        references dbtable(id)
                        on delete cascade
                        on update cascade,
                    name text not null,
                    constraint db_field_unique
                        unique (table_id, name) 
                        on conflict rollback
                )
            """)

    # some convenience 'getters'
    def tables(self, db):
        db_key = self.get_db_key(db.url)
        return self.metadata[db_key]['tables']

    def fields(self, db):
        db_key = self.get_db_key(db.url)
        return self.metadata[db_key]['fields']        
    def dottedfields(self, db):
        db_key = self.get_db_key(db.url)
        return self.metadata[db_key]['dottedfields']

    def write(self, sqconn, db_key, table, field):
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
                insert into dbfield(table_id, name) values (
                    :table_id, :field)""",
                dict(table_id=table_id, field=field))
        except sqlite3.IntegrityError:
            pass #already exists

