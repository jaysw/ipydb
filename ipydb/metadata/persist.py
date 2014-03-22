"""Persists (and reads) SQLAlchemy metadata representations to a local db.

write_* functions can be used to covert sqlalchemy.MetaData objects into
their simpler counterparts (in ipydb.model) and make them persisted to
a local sqlalchemy session.
"""

from sqlalchemy import orm

from ipydb.metadata import model as m


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
