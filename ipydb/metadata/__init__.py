# -*- coding: utf-8 -*-

"""
Reading and caching command-line completion strings from
a database schema.

:copyright: (c) 2012 by Jay Sweeney.
:license: see LICENSE for more details.
"""
from __future__ import print_function

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
try:
    from IPython.paths import locate_profile
except ImportError:
    # IPython 3 support
    from IPython.utils.path import locate_profile

from ipydb.utils import timer
from . import model as m
from . import persist

# invalidate db metadata if it is older than CACHE_MAX_AGE
MAX_CACHE_AGE = dt.timedelta(minutes=180)

log = logging.getLogger(__name__)

Session = orm.sessionmaker()


def get_metadata_engine(other_engine):
    """Create and return an SA engine for which will be used for
    storing ipydb db metadata about the input engine.

    Args:
        other_engine - SA engine for which we will be storing metadata for.
    Returns:
        tuple (dbname, sa_engine). dbname is a unique key for the input
        other_engine. sa_engine is the SA engine that will be used for storing
        metadata about `other_engine`
    """
    path = os.path.join(locate_profile(), 'ipydb')
    if not os.path.exists(path):
        os.makedirs(path)
    dbfilename = get_db_filename(other_engine)
    dburl = u'sqlite:////%s' % os.path.join(path, dbfilename)
    return dbfilename, sa.create_engine(dburl)


def get_db_filename(engine):
    """For the input SqlAlchemy engine, return a string name suitable for
    creating an sqlite database to store the engine's ipydb metadata.
    """
    url = engine.url
    url = str(URL(url.drivername, url.username, host=url.host,
                  port=url.port, database=url.database))
    return str(base64.urlsafe_b64encode(url.encode('utf-8')))


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
    for large oracle schemas). The approach taken here is to reflect and
    update database metadata in a background thread prevent ipydb
    from becoming unresponsive to the user.
    """

    pool = ThreadPool(multiprocessing.cpu_count() * 2)
    debug = False

    def __init__(self):
        self.databases = defaultdict(m.Database)

    def read_expunge(self, ipydb_engine):
        with session_scope(ipydb_engine) as session, \
                timer('Read-Expunge', log=log):
            db = persist.read(session)
            session.expunge_all()  # unhook SA
        return db

    def get_metadata(self, engine, noisy=False, force=False, do_reflection=True):
        """Fetch metadata for an sqlalchemy engine"""
        db_key, ipydb_engine = get_metadata_engine(engine)
        create_schema(ipydb_engine)
        db = self.databases[db_key]
        if db.reflecting:
            log.debug('Is already reflecting')
            # we're already busy
            return db
        if not do_reflection:
            return db
        if force:
            log.debug('was foreced to re-reflect')
            # return sqlite data, re-reflect
            db = self.read_expunge(ipydb_engine)
            self.databases[db_key] = db
            if noisy:
                print("ipydb is fetching database metadata")
            self.spawn_reflection_thread(db_key, db, engine.url)
            return db
        if db.age > MAX_CACHE_AGE:
            log.debug('Cache expired age:%s reading from sqlite', db.age)
            # read from sqlite, should be fast enough to do synchronously
            db = self.read_expunge(ipydb_engine)
            self.databases[db_key] = db
            if db.age > MAX_CACHE_AGE:
                log.debug('Sqlite data too old: %s, re-reflecting', db.age)
                # Metadata is empty or too old.
                # Spawn a thread to do the slow sqlalchemy reflection,
                # return whatever we have
                if noisy:
                    print("ipydb is fetching database metadata")
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
        db.reflecting = True
        target_engine = sa.create_engine(dburl_to_reflect)
        db_key, ipydb_engine = get_metadata_engine(target_engine)
        db.sa_metadata.bind = target_engine
        with timer('sa reflect', log=log):
            db.sa_metadata.reflect()
        with timer('drop-recreate schema', log=log):
            delete_schema(ipydb_engine)
            create_schema(ipydb_engine)
        with timer('Persist sa data', log=log):
            persist.write_sa_metadata(ipydb_engine, db.sa_metadata)
        # make sure that everything was eager loaded, and update
        # db metadata from other thread XXX: dicey
        with session_scope(ipydb_engine) as session:
            with timer('read-expunge after write', log=log):
                database = persist.read(session)
                db.update_tables(database.tables.values())
                db.sa_metadata = database.sa_metadata
                session.expunge_all()  # unhook SA
        db.reflecting = False

    def flush(self, engine):
        """Delete all metadata associated with engine."""
        self.pool.terminate()
        self.pool.join()
        db_key, ipydb_engine = get_metadata_engine(engine)
        del self.databases[db_key]
        delete_schema(ipydb_engine)
        create_schema(ipydb_engine)
        self.pool = ThreadPool(multiprocessing.cpu_count() * 2)

    def reflecting(self, engine):
        db_key = get_db_filename(engine)
        return self.databases[db_key].reflecting
