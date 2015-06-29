"""Persists (and reads) SQLAlchemy metadata representations to a local db."""
import logging

import sqlalchemy as sa
from sqlalchemy import orm

from ipydb.metadata import model as m

log = logging.getLogger(__name__)


def get_viewdata(metadata):
    views = []
    try:
        res = metadata.bind.execute(
            '''
            select
                table_name
            from
                information_schema.views
            where
                table_schema = 'public'
            ''')
        views = [{'name': row.table_name, 'isview': True} for row in res]
    except sa.exc.OperationalError:
        log.debug('Error fetching view from information_schema', exc_info=1)
    return views


def write_sa_metadata(engine, sa_metadata):
    """Bulk import of SqlAlchemy metadata into sqlite engine.

    We can assume that engine is a bunch of empty tables, hence
    should not need to do upsert/existence checking.
    Args:
        engine - SA engine for the ipydb sqlite db
        sa_metadata - bound metadata object for the
                      currently connected user db.
    """
    data = [{'name': t.name} for t in sa_metadata.sorted_tables]
    if data:
        engine.execute(m.Table.__table__.insert(), data)
    viewdata = get_viewdata(sa_metadata)
    if viewdata:
        engine.execute(m.Table.__table__.insert(), viewdata)
    result = engine.execute('select name, id from dbtable')
    tableidmap = dict(result.fetchall())

    def get_column_data(table):
        for column in table.columns:
            data = {
                'table_id': tableidmap[table.name],
                'name': column.name,
                'type': str(column.type),
                'primary_key': column.primary_key,
                'default_value': column.default,
                'nullable': column.nullable
            }
            yield data

    def all_col_data():
        for t in sa_metadata.sorted_tables:
            for coldata in get_column_data(t):
                yield coldata
        for vdata in viewdata:
            view = sa.Table(vdata['name'], sa_metadata, autoload=True)
            for coldata in get_column_data(view):
                yield coldata
    # XXX: SA doesn't like a generator?
    data = list(all_col_data())
    if data:
        engine.execute(m.Column.__table__.insert(), data)
    result = engine.execute(
        """
            select
                t.name || c.name,
                c.id
            from
                dbcolumn c
                inner join dbtable t on t.id = c.table_id
        """)
    columnidmap = dict(result.fetchall())

    def get_index_data():
        for table in sa_metadata.sorted_tables:
            for index in table.indexes:
                yield {
                    'name': index.name,
                    'unique': index.unique,
                    'table_id': tableidmap[table.name],
                }
    data = list(get_index_data())
    if data:
        engine.execute(m.Index.__table__.insert(), data)
    result = engine.execute(
        """
            select
                t.name || i.name,
                i.id
            from
                dbindex i
                inner join dbtable t on t.id = i.table_id
        """)
    indexidmap = dict(result.fetchall())

    def get_index_column_data():
        for table in sa_metadata.sorted_tables:
            for index in table.indexes:
                for column in index.columns:
                    index_id = indexidmap[table.name + index.name]
                    column_id = columnidmap[table.name + column.name]
                    yield {
                        'dbindex_id': index_id,
                        'dbcolumn_id': column_id
                    }
    ins = m.index_column_table.insert()
    ins.values(dbindex_id=sa.bindparam('dbindex_id'),
               dbcolumn_id=sa.bindparam('dbcolumn_id'))

    data = list(get_index_column_data())
    if data:
        engine.execute(ins, data)

    def get_fk_data():
        for table in sa_metadata.sorted_tables:
            for column in table.columns:
                for fk in column.foreign_keys:
                    column_id = columnidmap[table.name + column.name]
                    reftable_name = fk.column.table.name
                    ref_column_id = columnidmap[reftable_name + fk.column.name]
                    constraint_name = fk.constraint.name
                    yield {
                        'column_id': column_id,
                        'referenced_column_id': ref_column_id,
                        'constraint_name': constraint_name,
                    }
                    break  # XXX: only one per fk field for now!
    col = m.Column.__table__
    upd = col.update().\
        where(col.c.id == sa.bindparam('column_id')).\
        values(
            referenced_column_id=sa.bindparam('referenced_column_id'),
            constraint_name=sa.bindparam('constraint_name'))
    data = list(get_fk_data())
    if data:
        engine.execute(upd, data)


def read(session):
    tables = session.query(m.Table).\
        options(
            orm.joinedload('columns')
            .joinedload('referenced_by'),
            orm.joinedload('columns')
            .joinedload('referenced_column'),
            orm.joinedload('indexes')
            .joinedload('columns')
        ).all()
    # XXX: for some reason this is the only way that I could
    # force eager-loading of the column.referenced_column,
    # no idea why or how else to do it.
    for t in tables:
        for c in t.columns:
            if c.referenced_column:
                c.referenced_column.table
            for r in c.referenced_by:
                r.table
    return m.Database(tables=tables)
