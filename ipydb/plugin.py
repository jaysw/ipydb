# -*- coding: utf-8 -*-

"""
The ipydb plugin 

:copyright: (c) 2012 by Jay Sweeney.
:license: see LICENSE for more details.
"""
from ConfigParser import ConfigParser
from collections import defaultdict
import csv
import itertools
import fnmatch
import os
import sys
import sqlalchemy as sa
from IPython.core.plugin import Plugin
from sqlalchemy.sql.compiler import RESERVED_WORDS
from termsize import termsize
from magic import SqlMagics
from metadata import CompletionDataAccessor
from ipydb import CONFIG_FILE, PLUGIN_NAME

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

class SqlPlugin(Plugin):
    max_fieldsize = 100 # configurable?
    completion_data = CompletionDataAccessor()
    sqlformats = "table csv".split()
    not_connected_message = "ipydb is not connected to a database. "\
            "Try:\n\t%connect CONFIGNAME\nor try:\n\t"\
            "%connect_url dbdriver://user:pass@host/dbname\n"

    def __init__(self, shell=None, config=None):
        super(SqlPlugin, self).__init__(shell=shell, config=config)
        self.auto_magics = SqlMagics(self, shell)
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

    def show_tables(self, *globs):
        if not self.connected:
            print self.not_connected_message
            return
        matches = set()
        tablenames = self.completion_data.tables(self.engine)
        if not globs:
            matches = tablenames
        else:
            for glob in globs:
                matches.update(fnmatch.filter(tablenames, glob))
        print '\n'.join(sorted(matches))

    def show_fields(self, *globs):
        if not self.connected:
            print self.not_connected_message
            return
        matches = set()
        dottedfields = self.completion_data.dottedfields(self.engine)
        if not globs:
            matches = dottedfields
        for glob in globs:
            matches.update(fnmatch.filter(dottedfields, glob))
        tprev = None
        for match in sorted(matches):
            tablename, fieldname = match.split('.', 1)
            if tablename != tprev:
                if tprev is not None:
                    print
                print tablename
                print '-' * len(tablename)
            meta = sa.MetaData(bind=self.engine)
            meta.reflect(only=[tablename])  # XXX: this is slow...
            table = meta.tables[tablename]
            column = None
            for col in table.columns: # completion data is lcased...
                if col.name.lower() == fieldname:
                    column = col
                    break
            if column is not None:
                print "    %-35s%s" % (column.name, column.type)
            tprev = tablename
        print

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

