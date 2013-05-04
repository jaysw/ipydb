# -*- coding: utf-8 -*-

"""
Reading and caching command-line completion strings from
a database schema.

:copyright: (c) 2012 by Jay Sweeney.
:license: see LICENSE for more details.
"""

from collections import defaultdict, namedtuple
import datetime
from datetime import timedelta
from dateutil import parser
import logging
import multiprocessing
from multiprocessing.pool import ThreadPool
import os

import sqlalchemy as sa
from sqlalchemy.engine.url import URL
from IPython.utils.path import locate_profile

from ipydb.utils import timer

CACHE_MAX_AGE = 60 * 10  # invalidate connection metadata if
                         # it is older than CACHE_MAX_AGE

log = logging.getLogger(__name__)

fkclass = namedtuple('ForeignKey', 'table columns reftable refcolumns')


class ForeignKey(fkclass):
    __slots__ = ()

    def __str__(self):
        return '%s(%s) references %s(%s)' % (
            self.table, ','.join(self.columns),
            self.reftable, ','.join(self.refcolumns))

    def as_join(self):
        """Return a join statement representation of an foreign key.

        Returns:
            string: "a inner join b on a.f = b.g..."
        """
        joinstr = '%s inner join %s on ' % (self.reftable, self.table)
        sep = ''
        for idx, col in enumerate(self.columns):
            joinstr += sep + '%s.%s = %s.%s' % (
                self.reftable, self.refcolumns[idx],
                self.table, col)
            sep = ' and '
        return joinstr

pkclass = namedtuple('PrimaryKey', 'table columns')


class PrimaryKey(pkclass):
    __slots__ = ()

    def __str__(self):
        return "primary key %s (%s)" % (self.table, ','.join(self.columns))


class MetaData(object):

    def __init__(self):
        self.isempty = True
        self.reflecting = False
        self.created = datetime.datetime(datetime.MINYEAR, 1, 1)
        self._tables = set()
        self._fields = set()
        self._dottedfields = set()
        self._types = dict()
        self._foreign_keys = []
        self._primary_keys = []
        self.sa_metadata = sa.MetaData()

    def get_fields(self, table=None):
        if table:
            return [df.split('.')[1] for df in self.dottedfields
                    if df.startswith(table + '.')]
        return self.fields

    def get_dottedfields(self, table=None):
        if table:
            return [df for df in self.dottedfields
                    if df.startswith(table + '.')]
        return self.dottedfields

    def get_foreignkeys(self, table):
        """Return foreign keys for a table.

        Args:
            table: table name."""
        return [fk for fk in self.foreign_keys if fk.table == table]

    def get_primarykey(self, table):
        """Return primary key for a table.

        Args:
            table: table name."""
        for k in self.primary_keys:
            if k.table == table:
                return k

    def tables_referencing(self, table):
        """Return a set of table names reference a given table name.

        Args:
            table: Name of table.
        Returns:
            Set of table names that refence input table name.
        """
        refs = set()
        for fk in self.foreign_keys:
            if table == fk.table:
                refs.add(fk.reftable)
            elif table == fk.reftable:
                refs.add(fk.table)
        return refs

    def fields_referencing(self, table, field=None):
        refs = []
        for fk in self.foreign_keys:
            if table == fk.reftable:
                if field and field in fk.refcolumns:
                    refs.append(fk)
                elif not field:
                    refs.append(fk)
        return refs

    def get_all_joins(self, table):
        """Return all possible joins (fks) to and from a table.

        Args:
            table - return joins for table
        Returns:
            list fk's that represent joins to or from the given table.
        """
        refs = []
        for fk in self.foreign_keys:
            if table in (fk.reftable, fk.table):
                refs.append(fk)
        return refs

    def get_joins(self, t1, t2):
        """Return foreign_keys that can join two tables.

        Args:
            t1: First table name.
            t2: Second table name.
        Returns:
            A List of ForeignKey named tuples between the two tables.
        """
        joins = []
        for fk in self.foreign_keys:
            if t1 in (fk.table, fk.reftable) and \
                    t2 in (fk.table, fk.reftable):
                joins.append(fk)
        return joins

    @property
    def tables(self):
        return self._tables

    @tables.setter
    def tables(self, value):
        self._tables = value

    @property
    def fields(self):
        return self._fields

    @fields.setter
    def fields(self, value):
        self._fields = value

    @property
    def dottedfields(self):
        return self._dottedfields

    @dottedfields.setter
    def dottedfields(self, value):
        self._dottedfields = value

    @property
    def types(self):
        return self._types

    @types.setter
    def types(self, value):
        self._types = value

    @property
    def foreign_keys(self):
        return self._foreign_keys

    @foreign_keys.setter
    def foreign_keys(self, value):
        self._foreign_keys = value

    @property
    def primary_keys(self):
        return self._primary_keys

    @primary_keys.setter
    def primary_keys(self, value):
        self._primary_keys = value

    def __getitem__(self, key):
        # XXX: temporary back-compat hack
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)


class CompletionDataAccessor(object):
    """Reads and writes db-completion data from/to an sqlite db."""

    pool = ThreadPool(multiprocessing.cpu_count() * 2)
    dburl = 'sqlite:////%s' % os.path.join(locate_profile(), 'ipydb.sqlite')

    def __init__(self):
        self.metadata = defaultdict(MetaData)
        self.db = sa.engine.create_engine(self.dburl)
        self.create_schema(self.db)

    def get_metadata(self, db, noisy=False, force=False):
        db_key = self.get_db_key(db.url)
        metadata = self.metadata[db_key]
        if metadata.isempty and not force:
            has_metadata = self.has_metadata(db_key)
            if has_metadata:
                log.debug('Reading metadata from sqlite')
                self.read(db_key)  # XXX is this slow? make async?
                metadata.isempty = False
        now = datetime.datetime.now()
        if ((force or metadata.isempty or
                (now - metadata['created']) > timedelta(seconds=CACHE_MAX_AGE))
                and not metadata['reflecting']):
            log.debug('Reflecting db data from SA')
            metadata['reflecting'] = True
            self.pool.apply_async(self.reflect_metadata, (db,))
        return metadata

    def get_db_key(self, url):
        """Minimal unique key for describing a db connection."""
        return str(URL(url.drivername, url.username, host=url.host,
                   port=url.port, database=url.database))

    def has_metadata(self, db_key):
        """Return True if we have some stored data for db_key."""
        res = self.db.execute('select count(*) as c from dbtable '
                              'where db_key = :db_key', db_key=db_key)
        return bool(res.fetchone().c)

    def reflect_metadata(self, target_db):
        db_key = self.get_db_key(target_db.url)
        md = self.metadata[db_key]
        md.sa_metadata.bind = target_db
        with timer('sa reflect', log=log):
            md.sa_metadata.reflect()
        for table in md.sa_metadata.sorted_tables:
            with timer('reflect and save %s' % table.name, log=log):
                self.reflect_table(target_db, db_key, table)
                md.tables.add(table.name)
        self.metadata[db_key]['created'] = datetime.datetime.now()
        self.metadata[db_key]['reflecting'] = False

    def reflect_table(self, target_db, db_key, table):
        db_key = self.get_db_key(target_db.url)
        md = self.metadata[db_key]
        log.debug('reflect_table writing to md instance: %r', md)
        tablename = table.name.lower()
        md.isempty = False
        fks = {}
        for col in table.columns:
            fieldname = col.name.lower()
            dottedname = tablename + '.' + fieldname
            md.fields.add(fieldname)
            md.dottedfields.add(dottedname)
            md.types[dottedname] = str(col.type)
            constraint_name, pos, reftable, refcolumn = \
                self._get_foreign_key_info(col)
            if refcolumn:
                if constraint_name not in fks:
                    fks[constraint_name] = {
                        'table': tablename,
                        'columns': [],
                        'referenced_table': reftable,
                        'referenced_columns': []
                    }
                fks[constraint_name]['columns'].append(col.name)
                fks[constraint_name]['referenced_columns'].append(refcolumn)
        all_fks = md.foreign_keys
        for name, dct in fks.iteritems():
            fk = ForeignKey(dct['table'], dct['columns'],
                            dct['referenced_table'],
                            dct['referenced_columns'])
            try:
                all_fks.remove(fk)
            except ValueError:
                pass
            all_fks.append(fk)
        pk_cols = [c.name for c in table.primary_key.columns]
        md.primary_keys.append(PrimaryKey(table.name, pk_cols))
        self.write_table(self.db, db_key, table)

    def write_table(self, sqconn, db_key, table):
        """Writes information about a table to an sqlite db store.

        Args:
            sqcon: dbapi cursor to the metadata storage db.
            db_key: Key for the db that `table` belongs to.
            table: An sa.Table instance
        """
        with sqconn.begin() as sqtx:
            res = sqtx.execute(
                "select id from dbtable where db_key=:db_key and name=:table",
                dict(db_key=db_key, table=table.name))
            table_id = None
            row = res.fetchone()
            if row is not None:
                table_id = row[0]
            else:
                res = sqtx.execute(
                    """insert into dbtable(db_key, name) values (
                        :db_key, :table)""",
                    dict(db_key=db_key, table=table.name))
                table_id = res.lastrowid
            for column in table.columns:
                constraint_name, pos, reftable, refcolumn = \
                    self._get_foreign_key_info(column)
                column_id = None
                res = sqtx.execute(
                    "select id from dbfield where table_id=:table_id and name=:column_name",
                    dict(table_id=table_id, column_name=column.name))
                row = res.fetchone()
                if row is not None:
                    column_id = row.id
                if column_id is None:
                    sqtx.execute(
                        """
                            insert into dbfield(
                                table_id,
                                name,
                                type,
                                constraint_name,
                                position_in_constraint,
                                referenced_table,
                                referenced_column,
                                primary_key
                            ) values (
                                :table_id,
                                :field,
                                :type_,
                                :constraint_name,
                                :pos,
                                :reftable,
                                :refcolumn,
                                :primary_key
                            )
                        """,
                        dict(
                            table_id=table_id,
                            field=column.name,
                            type_=str(column.type),
                            constraint_name=constraint_name,
                            pos=pos,
                            reftable=reftable,
                            refcolumn=refcolumn,
                            primary_key=table.primary_key.contains_column(column)))
                else:
                    sqtx.execute(
                        """
                        update dbfield set
                            type = :type,
                            constraint_name = :constraint_name,
                            position_in_constraint = :pos,
                            referenced_table = :reftable,
                            referenced_column = :refcolumn,
                            primary_key = :primary_key
                        where
                            id = :column_id""",
                        dict(
                            column_id=column_id,
                            type=str(column.type),
                            constraint_name=constraint_name,
                            pos=pos,
                            reftable=reftable,
                            primary_key=table.primary_key.contains_column(column),
                            refcolumn=refcolumn))

    def read(self, db_key):
        fks = {}
        pks = {}
        result = self.db.execute("""
            select
                t.db_key,
                t.name as tablename,
                f.name as fieldname,
                f.type as type,
                constraint_name,
                position_in_constraint,
                referenced_table,
                referenced_column,
                f.primary_key
            from dbtable t inner join dbfield f
                on f.table_id = t.id
            where
                t.db_key = :db_key
        """, dict(db_key=db_key))
        for r in result:
            self.metadata[db_key].isempty = False
            self.metadata[db_key]['tables'].add(r.tablename)
            self.metadata[db_key]['fields'].add(r.fieldname)
            dottedfield = '%s.%s' % (r.tablename, r.fieldname)
            self.metadata[db_key]['dottedfields'].add(dottedfield)
            self.metadata[db_key]['types'][dottedfield] = r.type
            if r.primary_key:
                if r.tablename not in pks:
                    pk = PrimaryKey(r.tablename, [])
                    pks[r.tablename] = pk
                    self.metadata[db_key].primary_keys.append(pk)
                pks[r.tablename].columns.append(r.fieldname)

            if r.constraint_name:
                if r.constraint_name not in fks:
                    fks[r.constraint_name] = {
                        'table': r.tablename,
                        'columns': [],
                        'referenced_table': r.referenced_table,
                        'referenced_columns': []
                    }
                fks[r.constraint_name]['columns'].append(r.fieldname)
                fks[r.constraint_name]['referenced_columns'].append(
                    r.referenced_column)
        all_fks = []
        for name, dct in fks.iteritems():
            fk = ForeignKey(dct['table'],
                            dct['columns'],
                            dct['referenced_table'],
                            dct['referenced_columns'])
            all_fks.append(fk)
        self.metadata[db_key]['foreign_keys'] = all_fks
        result = self.db.execute("select max(created) as created from dbtable "
                                 "where db_key = :db_key",
                                 dict(db_key=db_key)).fetchone()
        if result[0]:
            self.metadata[db_key]['created'] = parser.parse(result[0])
        else:
            self.metadata[db_key]['created'] = datetime.datetime.now()

    def create_schema(self, sqconn):
        sqconn.execute("""
            create table if not exists dbtable (
                id integer primary key,
                db_key text not null,
                name text not null,
                created datetime not null default current_timestamp,
                constraint db_table_unique
                    unique (db_key, name)
                    on conflict rollback
            )
        """)
        sqconn.execute("""
            create table if not exists dbfield (
                id integer primary key,
                table_id integer not null
                    references dbtable(id)
                    on delete cascade
                    on update cascade,
                name text not null,
                type text,
                constraint_name text,
                position_in_constraint int,
                referenced_table text,
                referenced_column text,
                primary_key boolean,
                constraint db_field_unique
                    unique (table_id, name)
                    on conflict rollback
            )
        """)

    def flush(self):
        self.pool.terminate()
        self.pool.join()
        self.metadata = defaultdict(MetaData)
        self.delete_schema()
        self.create_schema(self.db)
        self.pool = ThreadPool(multiprocessing.cpu_count() * 2)

    def delete_schema(self):
        self.db.execute("""drop table dbfield""")
        self.db.execute("""drop table dbtable""")

    def reflecting(self, db):
        db_key = self.get_db_key(db.url)
        return self.metadata[db_key]['reflecting']

    def _get_foreign_key_info(self, column):
        constraint_name = None
        pos = None
        reftable = None
        refcolumn = None
        if len(column.foreign_keys):
            #  XXX: for now we pretend that there can only be one.
            fk = list(column.foreign_keys)[0]
            if fk.constraint:
                constraint_name = fk.constraint.name
                bits = fk.target_fullname.split('.')
                refcolumn = bits.pop()
                reftable = bits.pop()
                pos = 1  # XXX: this is incorrect
        return constraint_name, pos, reftable, refcolumn
