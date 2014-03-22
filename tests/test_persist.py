import logging

import sqlalchemy as sa
from nose import with_setup

from ipydb import metadata
from ipydb.metadata import model as m
from ipydb.metadata import persist


logging.basicConfig()

ipengine = None  # in memory ipydb engine with schema
ipsession = None

show_sql = False

if show_sql:
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


def setup_ipydb_schema():
    global ipsession, ipengine
    ipengine = sa.create_engine('sqlite:///:memory:')
    m.Base.metadata.create_all(ipengine)
    metadata.Session.configure(bind=ipengine)
    ipsession = metadata.Session()


def teardown_ipydb_schema():
    try:
        ipsession.commit()
    except Exception, e:
        ipsession.rollback()
        raise e
    finally:
        ipsession.close()
        ipengine.dispose()


def get_user_table():
    metadata = sa.MetaData()
    user = sa.Table(
        'user', metadata,
        sa.Column('user_id', sa.Integer, primary_key=True),
        sa.Column('user_name', sa.String(16), nullable=False),
        sa.Column('email_address', sa.String(60)),
        sa.Column('password', sa.String(20), nullable=False)
    )
    return user


@with_setup(setup_ipydb_schema, teardown_ipydb_schema)
def test_write_column_empty_schema():
    user = get_user_table()
    sacol = user.columns['user_id']
    user_table = m.Table(name=user.name)
    ipsession.add(user_table)

    column = persist.write_column(ipsession, user_table, sacol)

    assert column.name == sacol.name
    col2 = ipsession.query(m.Column).filter_by(
        name=sacol.name, table=user_table).scalar()
    assert column == col2
