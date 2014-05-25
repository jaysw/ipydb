"""Persists (and reads) SQLAlchemy metadata representations to a local db.

write_* functions can be used to covert sqlalchemy.MetaData objects into
their simpler counterparts (in ipydb.model) and make them persisted to
a local sqlalchemy session.
"""

import sqlalchemy as sa
from sqlalchemy import orm

from ipydb.metadata import model as m


def write_sa_metadata(engine, sa_metadata):
    """Bulk import of SqlAlchemy metadata into sqlite engine.

    We can assume that engine is a bunch of empty tables, hence
    should not need to do upsert/existence checking.
    """
    engine.execute(m.Table.__table__.insert(),
                   [{'name': t.name} for t in sa_metadata.sorted_tables])
    result = engine.execute('select name, id from dbtable')
    tableidmap = dict(result.fetchall())

    def get_column_data():
        for table in sa_metadata.sorted_tables:
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
    # XXX: SA doesn't like a generator?
    data = list(get_column_data())
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
    ins = m.Index.__table__.insert()
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


def write_table(session, satable):
    """Writes information about a table to an sqlite db store.

    Args:
        session - session on the metadata schema for an ipydb connection
        satable: An sa.Table instance
    Returns:
        A persistent ipydb.model.Table instance
    """
    table = session.query(m.Table).filter_by(name=satable.name).scalar()
    if not table:
        table = m.Table(name=satable.name)
        session.add(table)
    else:
        table.name = satable.name  # touch so that modified date is set
    for sacolumn in satable.columns:
        column = write_column(session, table, sacolumn)
        table.columns.append(column)
    for saindex in satable.indexes:
        index = write_index(session, table, saindex)
        table.indexes.append(index)
    return table


def write_column(session, table, sacolumn):
    column = session.query(m.Column).filter_by(name=sacolumn.name,
                                               table=table).scalar()
    data = {
        'table': table,
        'name': sacolumn.name,
        'type': str(sacolumn.type),
        'primary_key': sacolumn.primary_key,
        'default_value': sacolumn.default,
        'nullable': sacolumn.nullable
    }
    if not column:
        column = m.Column(**data)
        session.add(column)
    else:
        for k, v in data.iteritems():
            setattr(column, k, v)

    # recurse and add foreign keys
    for fk in sacolumn.foreign_keys:
        reftable = write_table(session, fk.column.table)
        refcolumn = write_column(session, reftable, fk.column)
        column.referenced_column = refcolumn
        column.constraint_name = fk.constraint.name
    return column


def write_index(session, table, saindex):
    """Pre: all indexes have a unique name: XXX: check this and
    all of the table's columns have already been persisted. """
    index = session.query(m.Index).filter_by(name=saindex.name,
                                             table=table).scalar()
    if not index:
        index = m.Index(name=saindex.name, table=table,
                        unique=saindex.unique)
        session.add(index)
    index.columns = []
    for sacolumn in saindex.columns:
        index.columns.append(table.column(sacolumn.name))
    return index


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
