# -*- coding: utf-8 -*-

"""
IPython extension to help you type and execute SQL queries

Usage:
    $ ipython
    In [1]: %load_ext ipydb            # ipydb needs to be loadable via sys.path
    In [2]: %connect_url mysql://user:pass@localhost/mydbname
    In [3]: select * from person order by id desc

:copyright: (c) 2012 by Jay Sweeney.
:license: see LICENSE for more details.
"""

__title__ = 'ipydb'
__version__ = '0.0.1'
__author__ = 'Jay Sweeney'
__license__ = 'artistic'
__copyright__ = 'Copyright 2012 Jay Sweeney'

import csv
import sys
import os
import itertools
import fnmatch
from ConfigParser import ConfigParser
from IPython.core.magic import Magics, magics_class, line_magic, line_cell_magic
from IPython.core.plugin import Plugin
from IPython.core.prompts import LazyEvaluate
from termsize import termsize
from collections import defaultdict
import sqlalchemy as sa
from sqlalchemy.sql.compiler import RESERVED_WORDS
from metadata import CompletionDataAccessor

CONFIG_FILE = os.path.join(os.path.expanduser('~'), '.db-connections')
PLUGIN_NAME = 'ipydb'
_loaded = False
_backup_prompt1 = ''

def load_ipython_extension(ip):
    """Load the extension in IPython."""
    global _loaded
    if not _loaded:
        plugin = SqlPlugin(shell=ip, config=ip.config)
        ip.plugin_manager.register_plugin(PLUGIN_NAME, plugin)
        configure_prompt(plugin)
        _loaded = True
        ipydb_help()

def configure_prompt(ipydb):
    global _backup_prompt1
    ip = ipydb.shell
    ip.prompt_manager.lazy_evaluate_fields['_ipydb'] = LazyEvaluate(ipydb.get_db_prompt1)
    tmpl = ip.prompt_manager.in_template
    _backup_prompt1 = tmpl
    tmpl = tmpl.rstrip(': ')
    tmpl += ' {color.Green}{_ipydb}: '
    ip.prompt_manager.in_template = tmpl

def ipydb_help():
    msg = "Welcome to ipydb %s!" % __version__
    print msg
    print
    msg2 = "ipydb has added the following `magic` commands to your ipython session:"
    print msg2
    helps = get_brief_help()
    maxname = max(map(len, (r[0] for r in helps)))
    print '-' * (maxname + 5)
    for magic, doc in helps:
        print ("    %%%-" + str(maxname) + "s    %s") % (magic, doc)
    print '-' * (maxname + 5)
    print
    print "You can get detailed usage information for any of the above commands "
    print "by typing %magic_command_name? For example, to get help on %connect, type"
    print
    print "    %connect?"
    print 
    print "Get started by connecting to a database using %connect_url or %connect"

def get_brief_help():
    """print first line of ipydb magic doc to show user 
    which magics are added by ipydb"""
    magics = []
    for magic in sorted(get_ipydb_magics()):
        m = getattr(SqlMagics, magic, None)
        if m:
            doc = getattr(m, '__doc__', '').strip()
            if not doc:
                doc = '<No Docstring>'
            magics.append((magic, doc.split('\n')[0]))
    return magics

def get_ipydb_magics():
    magics = {}
    magic_thing = SqlMagics.magics
    magics.update(magic_thing.get('cell', {}))
    magics.update(magic_thing.get('line', {}))
    return magics.keys()

def unload_ipython_extension(ip):
    # close any open connection / cursors..
    pass

def getconfigs():
    cp = ConfigParser()
    cp.read(CONFIG_FILE)
    configs = {}
    for section in cp.sections():
        conf = dict(cp.defaults())
        conf.update(dict(cp.items(section)))
        configs[section] = conf
    return configs

def ipydb_completer(self, text=None):
    """gets bound to IPython.core.completer.IPCompleter and called on tab-tab."""
    sqlplugin = self.shell.plugin_manager.get_plugin(PLUGIN_NAME)
    if sqlplugin:
        return sqlplugin.complete(self, text, self.line_buffer)
    else:
        return []

def sublists(l, n):
    return (l[i:i + n] for i in range(0, len(l), n))

def isublists(l, n):
    return itertools.izip_longest(*[iter(l)] * n)

def select_magic(arg):
    pass

@magics_class
class SqlMagics(Magics):

    def __init__(self, *a, **kw):
        super(SqlMagics, self).__init__(*a, **kw)

    @line_magic
    def ipydb_help(self, *args):
        """Show this help message"""
        ipydb_help()

    
    @line_cell_magic
    def sql(self, param='', cell=None):
        """Run an sql statement against the current ipydb connection

        Usage: %sql SQL_STATEMENT
        Example:

            %select id, first_name, last_name from person where first_name like 'J%'

        Also works as a multi-line ipython command. For example:

            %%sql
                select
                    id, name, description
                from
                    my_table
                where
                    id < 10

        """
        if cell is not None:
            param += '\n' + cell  
        result = self.plugin.execute(param)
        if result:
            self.plugin.render_result(result)

    @line_cell_magic
    def select(self, param='', cell=None):
        """Run a select statement against ipydb's current connection

        Example:

            %select id, name from things order by name desc

        If autocall is turned on, you can simply run the following:

            select * from my_table order by id desc

        Can be run as an ipython multi-line statement. For example:
            %%select first_name, last_name
            from 
                my_table
            where 
                foo = 'lur'
        """
        if cell is not None:
            param += '\n' + cell
        result = self.plugin.execute('select ' + param)
        if result:
            self.plugin.render_result(result)

    @line_magic
    def show_tables(self, param=''):
        """Show a list of tables for the current db connection 

        Usage: %show_tables [GLOB]

        Only show tables matching GLOB if given
        Example usage:
            %show_tables
                : lists all avaiable tables for the current connection
            %show_tables *p*
                : shows tables having a 'p' in their name

        """
        self.plugin.show_tables(param)

    @line_magic
    def show_fields(self, param=''):
        """Show a list of fields and data types for the given table

        Usage: %show_fields TABLE_GLOB[.FIELD_GLOB]

        Examples:
            show_fields person
                : shows fields for person table
            show_fields person.*id*
                : show fields for person table having `id` in their name
            show_fields *person*.*id*
                : show fields having id in their name for all tables
                  having 'person' in their name
        """
        def usage():
            return "Usage: %show_fields TABLE_PATTERN[.FIELD_PATTERN]" # XXX: extract
        if param:
            table = None
            field = None
            bits = param.split('.')
            if len(bits) == 1:
                table = bits[0]
            elif len(bits) == 2:
                table, field = bits
            else:
                print usage()
            self.plugin.show_fields(table, field)
        else:
            print usage()

    @line_magic
    def sqlformat(self, param=None):
        """Usage: %sqlformat <table|csv>"""
        if not param or param not in ('csv', 'table'):
            print self.sqlformat.__doc__
        else:
            self.plugin.sqlformat = param
            print "output format: %s" % self.plugin.sqlformat

    @line_magic
    def connect(self, param):
        """Connect to a database using ipydb's configuration file ~/.db-connections

        Usage: %connect NICKNAME

        For this to work, you need to create a file called
        ~/.db-connections. This file is an "ini" file, parsable by python's ConfigParser. 
        Here's an example of what ~/.db-connections might look like:

            [mydb]
            type: mysql
            username: root
            password: xxxx
            host: localhost
            database:timecollector

            [myotherdb]
            type: sqlite
            database: /path/to/file.sqlite
        
        Each database connection defined in ~/.db-connections is then referenceable 
        via its section heading, or NICKNAME. 

        You will need to ensure that you have installed a python driver for your chosen database.
        see: http://docs.sqlalchemy.org/en/rel_0_7/core/engines.html#supported-databases for recommended
        drivers.
        """
        self.plugin.connect(param)

    @line_magic
    def connect_url(self, param):
        """Connect to a database using an SqlAlchemy style connection URL

        Usage: %connect_url drivername://username:password@host/database
        Examples: 
            %connect_url mysql://root@localhost/mydatabase
            %connect_url sqlite:///:memory:
        You will need to ensure that you have installed a python driver for your chosen database.
        see: http://docs.sqlalchemy.org/en/rel_0_7/core/engines.html#supported-databases"""
        self.plugin.connect_url(param)

class SqlPlugin(Plugin):
    max_fieldsize = 100 # configurable?
    completion_data = CompletionDataAccessor()
    sqlformats = "table csv".split()
    not_connected_message = "ipydb is not connected to a database. "\
            "Try:\n\t%connect CONFIGNAME\nor try:\n\t"\
            "%connect_url dbdriver://user:pass@host/dbname\n"

    def __init__(self, shell=None, config=None):
        super(SqlPlugin, self).__init__(shell=shell, config=config)
        SqlMagics.plugin = self
        self.auto_magics = SqlMagics(shell)
        shell.register_magics(self.auto_magics)
        self.fieldnames = set()
        self.tablenames = set()
        self.dottedfieldnames = set()
        self.sqlformat = 'table' # 'table' | 'csv'
        self.sqlkeywords = set(RESERVED_WORDS)
        self.shell.set_custom_completer(ipydb_completer)
        self.reflecting = False
        self.thread = None
        self.connected = False
        self.engine = None

    def debug(self, *args):
        if self.shell.debug:
            print "DEBUG:%s" % ' '.join(map(str, args))

    def get_db_prompt1(self, *args, **kwargs):
        """return a string indicating current host/db for use by ipython.prompt_manager"""
        if not self.connected:
            return ''
        else:
            try:
                host = self.engine.url.host
                if '.' in host:
                    host = host.split('.')[0]
                host = host[:15] # don't like long hostnames
                db = self.engine.url.database[:15]
                return '%s/%s' % (host, db)
            except:
                return ''

    def safe_url(self, url_string):
        """return url_string with password removed or 
        None if url_string is not parseable"""
        url = None
        try:
            url = sa.engine.url.make_url(str(url_string))
            url.password = 'xxx'
            url = str(url)
        except:
            pass
        return url

    def connect(self, configname=None):
        """Connect to a database based upon its `nickname` 
        in the configuration file: ~/.db-connections"""
        configs = getconfigs()
        def available():
            print self.connect.__doc__
            print "Available config names: %s" % (' '.join(sorted(configs.keys())))
        if not configname:
            available()
        elif configname not in configs:
            print "Config `%s` not found. " % configname
            available()
        else:
            config = configs[configname]
            connect_args = {}
            if config['type'] == 'oracle':
                # not sure why we need this hack - 
                # I think there's some weirdness 
                # with cx_oracle version i'm using. 
                import cx_Oracle
                orig = cx_Oracle.makedsn
                cx_Oracle.makedsn = lambda *args, **kwargs: orig(*args, **kwargs).replace('SID', 'SERVICE_NAME')
            elif config['type'] == 'mysql':
                import MySQLdb.cursors
                # use server-side cursors by default (does this work with myISAM?)
                connect_args={'cursorclass': MySQLdb.cursors.SSCursor}
            self.connect_url(self.make_connection_url(config), connect_args=connect_args)
        if self.connected:
            self.completion_data.get_metadata(self.engine) # lazy, threaded, persistent cache
        return self.connected

    def connect_url(self, url, connect_args={}):
        """Connect to a datasbase using an SqlAlchemy URL"""
        safe_url = self.safe_url(url)
        if safe_url:
            print "ipydb is connecting to: %s" % safe_url
        self.engine = sa.engine.create_engine(url, connect_args=connect_args)
        self.connected = True
        return True

    def make_connection_url(self, config):
        cfg = defaultdict(str)
        cfg.update(config)
        return '{type}://{username}:{password}@{host}/{database}'.format(
                type=cfg['type'], username=cfg['username'], password=cfg['password'], 
                host=cfg['host'], database=cfg['database'])

    def execute(self, query):
        result = None
        if not self.connected:
            print self.not_connected_message
        else:
            result = self.engine.execute(query)
        return result

    def show_tables(self, pattern=None):
        if not pattern:
            pattern = '*' # matchall
        if not self.connected:
            print self.not_connected_message
        else:
            print '\n'.join(sorted(fnmatch.filter(self.engine.table_names(), pattern)))

    def show_fields(self, tablepattern, fieldpattern=None):
        orig_fieldpattern = fieldpattern
        if not self.connected:
            print self.not_connected_message
            return
        if not fieldpattern:
            fieldpattern = '*'
        matched = False
        for tablename in fnmatch.filter(self.engine.table_names(), tablepattern):
            print tablename
            print '-' * len(tablename)
            # XXX: this might not be thread-safe...
            # TODO: think about access here, and the CompletionDataAccessor...
            meta = sa.MetaData(bind=self.engine)
            meta.reflect(only=[tablename])  # XXX: check threadsafety
            table = meta.tables[tablename]
            for col in table.columns:
                if fnmatch.fnmatch(col.name.lower(),fieldpattern):
                    matched = True
                    print "    %-35s%s" % (col.name, col.type)
            print
        if not matched:
            msg = "No matches for %s" % tablepattern
            if orig_fieldpattern:
                msg += '.' + orig_fieldpattern
            print msg

    def render_result(self, result):
        try:
            out = os.popen('less -FXRiS','w') ## XXX: use ipython's pager abstraction
            if self.sqlformat == 'csv':
                self.format_result_csv(result, out=out)
            else:
                self.format_result_pretty(result, out=out)
        except IOError, msg:
            if msg.args == (32, 'Broken pipe'): # user quit
                pass
            else:
                raise
        finally:
            out.close()

    def format_result_pretty(self, result, out=sys.stdout):
        cols, lines = termsize()
        headings = result.keys()
        for screenrows in isublists(result, lines - 4):
            sizes = map(lambda x: len(str(x)), headings)
            for row in screenrows:
                if row is None: break
                sizes = map(max, zip(map(lambda x: min(self.max_fieldsize, len(str(x))), row), sizes))
            for size in sizes:
                out.write('+' + '-' * (size + 2))
            out.write('+\n')
            for idx, size in enumerate(sizes):
                fmt = '| %%-%is ' % size
                out.write(fmt % headings[idx])
            out.write('|\n')
            for size in sizes:
                out.write('+' + '-' * (size + 2))
            out.write('+\n')
            for rw in screenrows:
                if rw is None:
                    break # from isublists impl
                for idx, size in enumerate(sizes):
                    fmt = '| %%-%is ' % size
                    value = str(rw[idx])
                    if len(value) > self.max_fieldsize:
                        value = value[:self.max_fieldsize - 5] + '[...]'
                    value = value.replace('\n', r'^').replace('\r', r'^').replace('\t', ' ')
                    out.write(fmt % value)
                out.write('|\n')

    def format_result_csv(self, result, out=sys.stdout):
        writer = csv.writer(out)
        writer.writerow(result.keys())
        writer.writerows(result)


    def interested_in(self, completer, text, line_buffer=None):
        """return True if ipydb should try to do completions on the current line_buffer
        otherwise return False. 

        :completer is IPython.core.completer.IPCompleter
        `text` is the current token of text being completed """
        if text and not line_buffer:
            return True # this is unfortunate...
        else:
            first_token = line_buffer.split()[0].lstrip('%')
            return first_token in "show_fields connect sql select insert update delete sqlformat".split()

    def complete(self, completer, text, line_buffer=None):
        matches = []
        matches_append = matches.append
        self.debug('Completion requested on line:', line_buffer, 'token', text)
        if not self.interested_in(completer, text, line_buffer):
            self.debug('was not interested in [', line_buffer, ']')
            return []
        first_token = None
        if line_buffer:
            first_token = line_buffer.split()[0].lstrip('%')
        if first_token == 'connect':
            self.match_lists([getconfigs().keys()], text, matches_append)
            return matches
        if first_token == 'sqlformat':
            self.match_lists([self.sqlformats], text, matches_append)
            return matches
        return self.complete_sql(completer, text, line_buffer, first_token)

    def match_lists(self, lists, text, appendfunc):
        n = len(text)
        for word in itertools.chain(*lists):
            if word[:n] == text:
                appendfunc(word)

    def complete_sql(self, completer, text, line_buffer=None, first_token=None):
        """:completer is IPython.core.completer.IPCompleter
        `text` is the current token of text being completed """
        # todo: check state: is connected, has metdata etc. 
        # text_until_cursor = completer.text_until_cursor
        # if typing an as-yet undefined alias, then search all fieldnames
        if not self.connected:
            self.debug('bailing - not connected')
            return []
        matches = []
        matches_append = matches.append
        metadata = self.completion_data.get_metadata(self.engine, noisy=False)
        dottedfields = metadata['dottedfields']
        fields = metadata['fields']
        tables = metadata['tables']

        if text.count('.') == 1:
            head, tail = text.split('.')
            # todo: check that head is a valid keyword / tablename, alias etc
            #       and not something like 1.35<tab>
            # todo: parse aliases ahead of cursor, 
            # use them for dottedfieldname search.
            self.match_lists([dottedfields], text, matches_append)
            if not len(matches):
                # try for any field (following), could be 
                # table alias that is not yet defined 
                # (e.g. user typed `select foo.id, foo.<tab>...`)
                self.match_lists([tables], tail, 
                        lambda match: matches_append(head + '.' + match))
                if tail == '':
                    fields = map(lambda word: head + '.' + word, fields)
                    matches.extend(fields)
                return matches
        self.match_lists([tables, fields, self.sqlkeywords], 
                        text, matches_append)
        return matches





