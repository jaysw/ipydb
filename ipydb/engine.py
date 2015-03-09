# -*- coding: utf-8 -*-

"""Functions to help create an SQLalchemy connection based upon
a 'connection configuration file'"""
from future.standard_library import install_aliases
install_aliases()

from urllib import parse
from configparser import ConfigParser, DuplicateSectionError

import sqlalchemy as sa

from ipydb import CONFIG_FILE


def getconfigparser():
    cp = ConfigParser()
    cp.read(CONFIG_FILE)
    return cp


def getconfigs():
    """Return a dictionary of saved database connection configurations."""
    cp = getconfigparser()
    configs = {}
    default = None
    for section in cp.sections():
        conf = dict(cp.defaults())
        conf.update(dict(cp.items(section)))
        if conf.get('default'):
            default = section
        configs[section] = conf
    return default, configs


def get_nicknames():
    return sorted(getconfigs().keys())


def from_config(configname=None):
    """Connect to a database based upon its `nickname`.

    See ipydb.magic.connect() for details.
    """
    default, configs = getconfigs()

    if not configname:
        raise ValueError('Configname is required')
    elif configname not in configs:
        raise ValueError(
            'Config name not found. Try one of {%s}' % (get_nicknames()))
    else:
        config = configs[configname]
        connect_args = {}
        engine = from_url(make_connection_url(config),
                          connect_args=connect_args)
    return engine


def from_url(url, connect_args={}):
    """Connect to a database using an SqlAlchemy URL.

    Args:
        url: An SqlAlchemy-style DB connection URL.
        connect_args: extra argument to be passed to the underlying
                      DB-API driver.
    Returns:
        True if connection was successful.
    """
    url_string = url
    url = sa.engine.url.make_url(str(url_string))
    if url.drivername == 'oracle':
        # not sure why we need this horrible _cxmakedsn hack -
        # I think there's some weirdness
        # with cx_oracle/oracle versions I'm using.
        import cx_Oracle
        if not getattr(cx_Oracle, '_cxmakedsn', None):
            setattr(cx_Oracle, '_cxmakedsn', cx_Oracle.makedsn)

            def newmakedsn(*args, **kw):
                return cx_Oracle._cxmakedsn(*args, **kw).replace(
                    'SID', 'SERVICE_NAME')
            cx_Oracle.makedsn = newmakedsn
    elif url.drivername == 'mysql':
        import MySQLdb.cursors
        # use server-side cursors by default (does this work with myISAM?)
        connect_args = {'cursorclass': MySQLdb.cursors.SSCursor}
    engine = sa.engine.create_engine(url, connect_args=connect_args)
    return engine


def make_connection_url(config):
    """
    Returns an SqlAlchemy connection URL based upon values in config dict.

    Args:
        config: dict-like object with keys: type, username, password,
                host, and database.
    Returns:
        str URL which SqlAlchemy can use to connect to a database.
    """
    return sa.engine.url.URL(
        drivername=config.get('type'), username=config.get('username'),
        password=config.get('password'), host=config.get('host'),
        port=config.get('port') or None,
        database=config.get('database'),
        query=dict(parse.parse_qsl(config.get('query', ''))))


def save_connection(name, engine, overwrite=False):
    """Saves a connection configuration to ~/.db-connections."""
    cp = getconfigparser()
    try:
        cp.add_section(name)
    except DuplicateSectionError:
        if overwrite:
            pass
        else:
            raise
    url = engine.url
    cp.set(name, 'type', url.drivername or '')
    cp.set(name, 'username', url.username or '')
    cp.set(name, 'password', url.password or '')
    cp.set(name, 'host', url.host or '')
    cp.set(name, 'database', url.database or '')
    cp.set(name, 'port', url.port or '')
    cp.set(name, 'query', url.query or '')
    with open(CONFIG_FILE, 'w') as fout:
        cp.write(fout)
