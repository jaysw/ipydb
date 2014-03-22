import logging

import sqlalchemy as sa

from ipydb import metadata
from ipydb.metadata import model as m


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



def setup(*args, **kw):
    pass


def teardown(*args, **kw):
    pass

def test_get_metadata():
    #acc = metadata.MetaDataAccessor()
    #db = acc.get_metadata(engine, force=True)
    #assert 'entry' in db.tables.keys()
    pass


def test_eager_load():
    pass

def test_get_joins():
    pass
