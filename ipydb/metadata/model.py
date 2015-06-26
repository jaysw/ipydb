"""A simple SQLAlchemy model for describing database metadata.

Stores information about tables, columns, indexes, and foreign-keys.
Database (non persistent) gives a high-level API to a collection
of Tables objects from a given database schema.
"""
import collections
import datetime as dt
import itertools
import logging
import re

import future
from future.utils import viewvalues
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.declarative import declarative_base

ZERODATE = dt.datetime(dt.MINYEAR, 1, 1)
Base = declarative_base()
log = logging.getLogger(__name__)


class Database(object):
    """Database metadata for a particular database.

    Databases are identified by the sqlalchemy connection url
    without the password (dbkey) and contain a dictionary of
    model.Table objects keyed by table name.
    There be dragons: another thread can be writing
    to self.tables at any time, so we need to lock for
    reads/writes. (that would be XXX TODO)
    """

    def __init__(self, tables=None):
        self.tables = {}
        self.modified = None
        self.reflecting = False
        self.sa_metadata = sa.MetaData()
        if tables is None:
            tables = []
        self.update_tables(tables)

    def isempty(self):
        return bool(self.tables)

    def update_tables(self, tables):
        """Update table definitions from a list of tables."""
        for t in tables:
            self.isempty = False
            self.tables[t.name] = t
            if self.modified is None:
                self.modified = t.modified
            self.modified = min(self.modified, t.modified,
                                key=lambda x: '' if x is None else x)

    @property
    def views(self):
        for t in viewvalues(self.tables):
            if t.isview:
                yield t

    def tablenames(self):
        return list(self.tables)

    @property
    def columns(self):
        for t in viewvalues(self.tables):
            for c in t.columns:
                yield c

    def fieldnames(self, table=None, dotted=False):
        ret = set()
        if table is None:  # all field names
            for t in viewvalues(self.tables):
                if dotted:
                    ret.update(['%s.%s' % (t.name, c.name) for c in t.columns])
                else:
                    ret.update([c.name for c in t.columns])

            return ret
        if table not in self.tables:
            return set()
        t = self.tables[table]
        if dotted:
            return {'%s.%s' % (t.name, c.name) for c in t.columns}
        return {c.name for c in t.columns}

    def get_joins(self, tbl1, tbl2):
        if tbl1 not in self.tables or tbl2 not in self.tables:
            return set()
        t1 = self.tables[tbl1]
        t2 = self.tables[tbl2]
        joins = set()
        for src, tgt in [(t1, t2), (t2, t1)]:
            for c in src.columns:
                if (c.referenced_column and
                        c.referenced_column.table.name == tgt.name):
                    joins.add(ForeignKey(
                        src.name, (c.name,),
                        tgt.name, (c.referenced_column.name,)))
        return joins

    def tables_referencing(self, tbl):
        if tbl not in self.tables:
            return set()
        reftables = set()
        for c in self.tables[tbl].columns:
            reftables.update({col.table.name for col in c.referenced_by})
        return reftables

    def fields_referencing(self, tbl, column=None):
        if tbl not in self.tables:
            raise StopIteration()
        for c in self.tables[tbl].columns:
            for r in c.referenced_by:
                if column is None or column == r.referenced_column.name:
                    yield ForeignKey(r.table.name, (r.name,), tbl, (c.name,))

    def foreign_keys(self, tbl):
        if tbl not in self.tables:
            raise StopIteration()
        for c in self.tables[tbl].columns:
            if c.referenced_column:
                yield ForeignKey(tbl, (c.name,),
                                 c.referenced_column.table.name,
                                 (c.referenced_column.name,))

    def all_joins(self, tbl):
        return itertools.chain(self.foreign_keys(tbl),
                               self.fields_referencing(tbl))

    def insert_statement(self, tbl):
        if tbl not in self.tables:
            return ''
        t = self.tables[tbl]
        sql = 'insert into {table} ({columns}) values ({defaults})'
        columns = ', '.join(c.name for c in t.columns)
        defaults = ', '.join(sql_default(c) for c in t.columns)
        return sql.format(table=tbl, columns=columns, defaults=defaults)

    @property
    def age(self):
        """return age of this metadata as a datetime.timedelta"""
        return dt.datetime.now() - (self.modified or ZERODATE)

    def indexes(self, tbl):
        if tbl not in self.tables:
            raise StopIteration()
        for index in self.tables[tbl].indexes:
            yield index


fkclass = collections.namedtuple('ForeignKey',
                                 'table columns reftable refcolumns')


class ForeignKey(fkclass):
    """Simplistic representation of a foreign key"""
    __slots__ = ()

    def __str__(self):
        return '%s(%s) references %s(%s)' % (
            self.table, ','.join(self.columns),
            self.reftable, ','.join(self.refcolumns))

    def as_join(self, reverse=False):
        """Return a string formatted as an SQL join expression."""
        tables = [self.reftable, self.table]
        if reverse:
            tables.reverse()
        tables = tuple(tables)
        joinstr = '%s inner join %s on ' % tables
        sep = ''
        for idx, col in enumerate(self.columns):
            joinstr += sep + '%s.%s = %s.%s' % (
                self.reftable, self.refcolumns[idx],
                self.table, col)
            sep = ' and '
        return joinstr


restr = re.compile(r'TEXT|VARCHAR.*|CHAR.*', re.I)
renumeric = re.compile(r'FLOAT.*|DECIMAL.*|INT.*|DOUBLE.*|'
                       'FIXED.*|SHORT.*|NUMBER.*|NUMERIC.*', re.I)
redate = re.compile(r'DATE|TIME|DATETIME|TIMESTAMP', re.I)


def sql_default(column):
    """Return an acceptable default value for the given column.
    col is an ipydb.model.Column.
    """
    if column.default_value:
        return "'%s'" % column.default_value
    if column.nullable:
        return 'NULL'
    typ = str(column.type).lower().strip()
    value = ''
    if redate.search(typ):
        log.debug('%s is a date', typ)
        head = typ.split()[0]
        if head == 'date':
            value = "current_date"
        elif head == 'time':
            value = "current_time"
        elif head in ('datetime', 'timestamp'):
            value = "current_timestamp"
    elif restr.search(typ):
        log.debug('%s is a string', typ)
        value = "'hello'"
    elif renumeric.search(typ):
        log.debug('%s is a number', typ)
        value = "0"
    else:
        log.debug('no match for type: %s', typ)
    return value


class TimesMixin(object):
    created = sa.Column(sa.DateTime, default=dt.datetime.now)
    modified = sa.Column(sa.DateTime, default=dt.datetime.now,
                         onupdate=dt.datetime.now)


class Table(Base, TimesMixin):
    __tablename__ = 'dbtable'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, index=True, unique=True)
    isview = sa.Column(sa.Boolean, default=False, nullable=False)

    def column(self, name):
        for column in self.columns:
            if column.name == name:
                return column
        else:
            raise KeyError("Column %s not found in table %s" %
                           (name, self.name))


class Column(Base):
    __tablename__ = 'dbcolumn'
    __table_args__ = (
        sa.UniqueConstraint('table_id', 'name'),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    table_id = sa.Column(sa.Integer, sa.ForeignKey('dbtable.id'))
    name = sa.Column(sa.String, index=True)
    type = sa.Column(sa.String)
    referenced_column_id = sa.Column(sa.Integer, sa.ForeignKey('dbcolumn.id'))
    constraint_name = sa.Column(sa.String, nullable=True)
    primary_key = sa.Column(sa.Boolean)
    nullable = sa.Column(sa.Boolean)
    default_value = sa.Column(sa.String, nullable=True)

    table = orm.relationship('Table', backref='columns', order_by=name)
    referenced_column = orm.relationship(
        'Column', backref='referenced_by', remote_side=[id])


index_column_table = sa.Table(
    'dbindex_dbcolumn', Base.metadata,
    sa.Column('dbindex_id', sa.Integer, sa.ForeignKey('dbindex.id')),
    sa.Column('dbcolumn_id', sa.Integer, sa.ForeignKey('dbcolumn.id')))


class Index(Base):
    __tablename__ = 'dbindex'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, index=True)
    unique = sa.Column(sa.Boolean)
    table_id = sa.Column(sa.Integer, sa.ForeignKey('dbtable.id'))

    table = orm.relationship('Table', backref='indexes', order_by=name)
    columns = orm.relationship('Column', secondary=lambda: index_column_table,
                               backref='indexes')
