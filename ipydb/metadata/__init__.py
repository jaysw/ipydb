# -*- coding: utf-8 -*-

"""
Reading and caching command-line completion strings from
a database schema.

:copyright: (c) 2012 by Jay Sweeney.
:license: see LICENSE for more details.
"""

import base64
from collections import defaultdict
from contextlib import contextmanager
import datetime as dt
import logging
import multiprocessing
from multiprocessing.pool import ThreadPool
import os

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.engine.url import URL
from IPython.utils.path import locate_profile

from ipydb.utils import timer
from . import model as m
from . import persist

MAX_CACHE_AGE = dt.timedelta(minutes=20)  # invalidate db metadata if
                                          # it is older than CACHE_MAX_AGE

log = logging.getLogger(__name__)

Session = orm.sessionmaker()


def engine_from_key(db_key):
    path = os.path.join(locate_profile(), 'ipydb')
    if not os.path.exists(path):
        os.makedirs(path)
    dburl = 'sqlite:////%s' % os.path.join(locate_profile(), 'ipydb', db_key)
    return sa.create_engine(dburl)


def get_db_key(engine):
    """Unique key for an sqlachemy db connection. url/filename safe"""
    url = engine.url
    url = str(URL(url.drivername, url.username, host=url.host,
                  port=url.port, database=url.database))
    return base64.urlsafe_b64encode(url)


@contextmanager
def session_scope(engine):
    """Provide a transactional scope around a series of operations."""
    Session.configure(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def create_schema(engine):
    m.Base.metadata.create_all(engine)


def delete_schema(engine):
    m.Base.metadata.drop_all(engine)


class MetaDataAccessor(object):
    """Reads and writes database metadata.

    Database metadata is read from sqlalchemy.MetaData, converted to
    a simplified schema (ipydb.metadata.model) and saved to an sqlite
    database for successive fast-loading. sqlalchemy.MetaData.reflect()
    can be very slow for large/complicated database schemas (this was observed
    for large oracle databases). The approach taken here is to reflect and
    update database metadata in a background thread prevent  ipydb
    from becoming unresponsive to the user.
    """

    pool = ThreadPool(multiprocessing.cpu_count() * 2)
    debug = True

    def __init__(self):
        self.databases = defaultdict(m.Database)

    def get_metadata(self, engine, noisy=False, force=False):
        """Fetch metadata for an sqlalchemy engine"""
        db_key = get_db_key(engine)
        ipydb_engine = engine_from_key(db_key)
        create_schema(ipydb_engine)
        db = self.databases[db_key]
        if db.reflecting:
            # we're already busy
            return db
        if force:
            # return sqlite data, re-reflect
            with session_scope(ipydb_engine) as session:
                db = self.read(session)
                session.expunge_all()  # unhook SA
            self.databases[db_key] = db
            if noisy:
                print "ipydb is fetching database metadata"
            self.spawn_reflection_thread(db_key, db, engine.url)
            return db
        if db.age > MAX_CACHE_AGE:
            # read from sqlite, should be fast enough to do synchronously
            with session_scope(ipydb_engine) as session:
                db = self.read(session)
                session.expunge_all()  # unhook SA
            self.databases[db_key] = db
            if db.age > MAX_CACHE_AGE:
                # Metadata is empty or too old.
                # Spawn a thread to do the slow sqlalchemy reflection,
                # return whatever we have
                if noisy:
                    print "ipydb is fetching database metadata"
                self.spawn_reflection_thread(db_key, db, engine.url)
        return db

    def spawn_reflection_thread(self, db_key, db, dburl_to_reflect):
        if not self.debug:
            self.pool.apply_async(self.reflect_db,
                                  (db_key, db, dburl_to_reflect))
        else:
            self.reflect_db(db_key, db, dburl_to_reflect)

    def reflect_db(self, db_key, db, dburl_to_reflect):
        """runs in a new thread"""
        ipydb_engine = engine_from_key(db_key)
        target_engine = sa.create_engine(dburl_to_reflect)
        db.sa_metadata.bind = target_engine
        with timer('sa reflect', log=log):
            db.sa_metadata.reflect()
        delete_schema(ipydb_engine)
        create_schema(ipydb_engine)
        with session_scope(ipydb_engine) as session:
            for satable in db.sa_metadata.sorted_tables:
                table = persist.write_table(session, satable)
                db.update_tables([table])
            # make sure that everything was eager loaded:
            self.read(session)
            # and detach
            session.expunge_all()  # unhook SA
        db.reflecting = False

    def flush(self, engine):
        self.pool.terminate()
        self.pool.join()
        del self.metadata[get_db_key(engine)]
        delete_schema(engine)
        create_schema(engine)
        self.pool = ThreadPool(multiprocessing.cpu_count() * 2)

    def reflecting(self, db):
        db_key = get_db_key(db.url)
        return self.metadata[db_key].reflecting
