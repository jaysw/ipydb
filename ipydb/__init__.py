# -*- coding: utf-8 -*-

"""
ipydb: An IPython extension to help you type and execute SQL queries.

Usage:
    $ ipython
    In [1]: %load_ext ipydb
    In [2]: %connect_url mysql://user:pass@localhost/mydbname
    In [3]: %select * from person order by id desc

:copyright: (c) 2012 by Jay Sweeney.
:license: see LICENSE for more details.
"""
__title__ = 'ipydb'
__version__ = '0.0.2-alpha'
__author__ = 'Jay Sweeney'
__license__ = 'Apache 2.0'
__copyright__ = 'Copyright 2012 Jay Sweeney'

import logging
import os

PLUGIN_NAME = 'ipydb'
_loaded = False
_backup_prompt1 = ''
CONFIG_FILE = os.path.join(os.path.expanduser('~'), '.db-connections')


def load_ipython_extension(ip):
    """Load the ipydb into the active ipython session"""
    from plugin import SqlPlugin
    global _loaded
    if not _loaded:
        plugin = SqlPlugin(shell=ip, config=ip.config)
        configure_prompt(plugin)
        _loaded = True
        ipydb_help()
        logging.basicConfig()


def configure_prompt(ipydb):
    from IPython.core.prompts import LazyEvaluate
    global _backup_prompt1
    ip = ipydb.shell
    ip.prompt_manager.lazy_evaluate_fields['_ipydb'] = LazyEvaluate(
        ipydb.get_db_ps1)
    ip.prompt_manager.lazy_evaluate_fields['_reflecting'] = LazyEvaluate(
        ipydb.get_reflecting_ps1)
    ip.prompt_manager.lazy_evaluate_fields['_tx'] = LazyEvaluate(
        ipydb.get_transaction_ps1)
    tmpl = ip.prompt_manager.in_template
    _backup_prompt1 = tmpl
    tmpl = tmpl.rstrip(': ')
    tmpl += '{color.LightPurple}{_reflecting}' \
            '{color.Cyan}{_ipydb}' \
            '{color.LightRed}{_tx}' \
            '{color.Green}: '
    ip.prompt_manager.in_template = tmpl


def ipydb_help():
    msg = "Welcome to ipydb %s!" % __version__
    print msg
    print
    msg2 = 'ipydb has added the following `magic` ' \
        'commands to your ipython session:'
    print msg2
    helps = get_brief_help()
    maxname = max(map(len, (r[0] for r in helps)))
    print '-' * (maxname + 5)
    for magic, doc in helps:
        print ("    %%%-" + str(maxname) + "s    %s") % (magic, doc)
    print '-' * (maxname + 5)
    print
    print "You can get detailed usage information " \
        "for any of the above commands "
    print "by typing %magic_command_name? For example, " \
        "to get help on %connect, type"
    print
    print "    %connect?"
    print
    print "Get started by connecting to a database " \
        "using %connect_url or %connect"


def get_brief_help():
    """return a list of (magic_name, first_line_of_docstring)
    for all the magic methods ipydb defines"""
    from magic import SqlMagics
    docs = []
    magics = {}
    magic_thing = SqlMagics.magics
    magics.update(magic_thing.get('cell', {}))
    magics.update(magic_thing.get('line', {}))
    for magic in sorted(magics.keys()):
        m = getattr(SqlMagics, magic, None)
        if m:
            if hasattr(m, '__description__'):
                doc = m.__description__
            else:
                doc = getattr(m, '__doc__', '')
                if doc is not None:
                    doc = doc.strip()
            if not doc:
                doc = '<No Docstring>'
            docs.append((magic, doc.split('\n')[0]))
    return docs


def unload_ipython_extension(ip):
    ip.prompt_manager.in_template = _backup_prompt1
    # XXX: close any open connection / cursors..
