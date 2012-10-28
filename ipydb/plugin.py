# -*- coding: utf-8 -*-

"""
The ipydb plugin. 

:copyright: (c) 2012 by Jay Sweeney.
:license: see LICENSE for more details.
"""
from ConfigParser import ConfigParser
from collections import defaultdict
import csv
import itertools
import fnmatch
import os
import re
import sys
import sqlalchemy as sa
from sqlalchemy.sql.compiler import RESERVED_WORDS
from IPython.core.plugin import Plugin
from termsize import termsize
from magic import SqlMagics
from metadata import CompletionDataAccessor
from ipydb import CONFIG_FILE, PLUGIN_NAME


def getconfigs():
    """Return a dictionary of saved database connection configurations."""
    cp = ConfigParser()
    cp.read(CONFIG_FILE)
    configs = {}
    for section in cp.sections():
        conf = dict(cp.defaults())
        conf.update(dict(cp.items(section)))
        configs[section] = conf
    return configs


def sublists(l, n):
    return (l[i:i + n] for i in range(0, len(l), n))


def isublists(l, n):
    return itertools.izip_longest(*[iter(l)] * n)


def ipydb_completer(self, text=None):
    """Returns a list of suggested completions for text.

    Note: This is bound to IPython.core.completer.IPCompleter 
          and called on tab-presses by ipython.
    Args:
        text: String of text to complete.
    Returns:
        A list of candidate strings which complete the input text"""
    sqlplugin = self.shell.plugin_manager.get_plugin(PLUGIN_NAME)
    if sqlplugin:
        return sqlplugin.complete(self, text, self.line_buffer)
    else:
        return []


class SqlPlugin(Plugin):
    """The ipydb plugin - manipulate databases from ipython."""

    max_fieldsize = 100 # configurable?
    completion_data = CompletionDataAccessor()
    sqlformats = "table csv".split()
    not_connected_message = "ipydb is not connected to a database. "\
                            "Try:\n\t%connect CONFIGNAME\nor try:\n\t"\
                            "%connect_url dbdriver://user:pass@host/dbname\n"

    def __init__(self, shell=None, config=None):
        """Constructor.

        Args:
            shell: An instance of IPython.core.InteractiveShell.
            config: IPython's config object.
        """
        super(SqlPlugin, self).__init__(shell=shell, config=config)
        self.auto_magics = SqlMagics(self, shell)
        shell.register_magics(self.auto_magics)
        self.sqlformat = 'table' # 'table' | 'csv'
        self.shell.set_custom_completer(ipydb_completer)
        self.connected = False
        self.engine = None
        self.nickname = None
        self.autocommit = True
        self.trans_ctx = None

    def get_db_ps1(self, *args, **kwargs):
        """Return a string indicating current host/db for use in ipython's prompt PS1."""
        if not self.connected:
            return ''
        host = self.engine.url.host
        if '.' in host:
            host = host.split('.')[0]
        host = host[:15] # don't like long hostnames
        db = self.engine.url.database[:15]
        url = "%s/%s" % (host, db)
        if self.nickname:
            url = self.nickname
        return " " + url

    def get_transaction_ps1(self, *args, **kw):
        """Return a string indicating the transaction state for use in ipython's prompt PS1."""
        if not self.connected:
            return ''
        # I want this: ⚡ 
        # but looks like IPython is expecting ascii for the PS1!? 
        return ' *' if self.trans_ctx and self.trans_ctx.transaction.is_active else ''

    def get_reflecting_ps1(self, *args, **kw):
        """Return a string indictor if background schema reflection is running."""
        if not self.connected:
            return ''
        return ' !' if self.completion_data.reflecting(self.engine) else ''
        
    def safe_url(self, url_string):
        """Return url_string with password removed or None if url_string is not parseable."""
        url = None
        try:
            url = sa.engine.url.make_url(str(url_string))
            url.password = 'xxx'
        except:
            pass
        return url

    def connect(self, configname=None):
        """Connect to a database based upon its `nickname`.
        
        See ipydb.magic.connect() for details. 
        """
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
            self.connect_url(self.make_connection_url(config), connect_args=connect_args)
            self.nickname = configname
        return self.connected

    @property
    def metadata(self):
        """Get sqlalchemy.MetaData instance for current connection."""
        if not self.connected:
            return None
        meta = getattr(self, '_metadata', None)
        if meta is None or self._metadata.bind != self.engine:
            self._metadata = sa.MetaData(bind=self.engine)
        return self._metadata

    def connect_url(self, url, connect_args={}):
        """Connect to a database using an SqlAlchemy URL.

        Args:
            url: An SqlAlchemy-style DB connection URL.
            connect_args: extra argument to be passed to the underlying
                          DB-API driver.
        Returns:
            True if connection was successful.
        """
        safe_url = self.safe_url(url)
        if safe_url:
            print "ipydb is connecting to: %s" % safe_url
        if safe_url.drivername == 'oracle':
            # not sure why we need this horrible hack - 
            # I think there's some weirdness 
            # with cx_oracle/oracle versions I'm using. 
            import cx_Oracle
            if not getattr(cx_Oracle, '_cxmakedsn', None):
                setattr(cx_Oracle, '_cxmakedsn', cx_Oracle.makedsn)
                cx_Oracle.makedsn = lambda *args, **kwargs: \
                                        cx_Oracle._cxmakedsn(*args, **kwargs).replace('SID', 'SERVICE_NAME')
        elif safe_url.drivername == 'mysql':
            import MySQLdb.cursors
            # use server-side cursors by default (does this work with myISAM?)
            connect_args={'cursorclass': MySQLdb.cursors.SSCursor}
        self.engine = sa.engine.create_engine(url, connect_args=connect_args)
        self.connected = True
        self.nickname = None
        self.completion_data.get_metadata(self.engine) # lazy, threaded, persistent cache
        return True

    def flush_metadata(self):
        """Delete cached schema information"""
        print "Deleting metadata..."
        self.completion_data.flush()
        if self.connected:
            self.completion_data.get_metadata(self.engine)

    def make_connection_url(self, config):
        """Returns an SqlAlchemy connection URL based upon values in `config` dict.

        Args:
            config: dict-like object with keys: type, username, password,
                    host, and database.
        Returns:
            str URL which SqlAlchemy can use to connect to a database.
        """
        cfg = defaultdict(str)
        cfg.update(config)
        return '{type}://{username}:{password}@{host}/{database}'.format(
                type=cfg['type'], username=cfg['username'], password=cfg['password'], 
                host=cfg['host'], database=cfg['database'])

    def execute(self, query):
        """Execute query against current db connection, return result set.

        Args:
            query: string query to execute
        Returns:
            Sqlalchemy's DB-API cursor-like object. 
        """
        result = None
        if not self.connected:
            print self.not_connected_message
        else:
            if self.trans_ctx and self.trans_ctx.transaction.is_active:
                result = self.trans_ctx.conn.execute(query)
            else:
                result = self.engine.execute(query)
        return result

    def begin(self):
        """Start a new transaction against the current db connection."""
        if not self.connected:
            print self.not_connected_message
            return
        if not self.trans_ctx or not self.trans_ctx.transaction.is_active:
            self.trans_ctx = self.engine.begin()
        else:
            print "You are already in a transaction block and nesting is not supported"

    def commit(self):
        """Commit current transaction if there was one."""
        if not self.connected:
            print self.not_connected_message
            return
        if self.trans_ctx:
            with self.trans_ctx:
                pass
            self.trans_ctx = None
        else:
            print "No active transaction"

    def rollback(self):
        """Rollback current transaction if there was one."""
        if not self.connected:
            print self.not_connected_message
            return
        if self.trans_ctx:
            self.trans_ctx.transaction.rollback()
            self.trans_ctx = None
        else:
            print "No active transaction"

    def show_tables(self, *globs):
        """Print a list of tablenames matching input glob/s.

        All table names are printed if no glob is given, otherwise
        just those table names matching any of the *globs are printed. 

        Args:
            *glob: zero or more globs to match against table names.

        """
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
        """Print a list of fields matching the input glob tableglob[.fieldglob].

        See ipydb.magic.show_fields for examples. 

        Args:
            *globs: list of [tableglob].[fieldglob] strings
        """
        if not self.connected:
            print self.not_connected_message
            return
        matches = set()
        dottedfields = self.completion_data.dottedfields(self.engine)
        if not globs:
            matches = dottedfields
        for glob in globs:
            bits = glob.split('.', 1)
            if len(bits) == 1: # table name only
                glob += '.*'
            matches.update(fnmatch.filter(dottedfields, glob))
        tprev = None
        for match in sorted(matches):
            tablename, fieldname = match.split('.', 1)
            if tablename != tprev:
                if tprev is not None:
                    print
                print tablename
                print '-' * len(tablename)
            print "    %-35s%s" % (fieldname, self.completion_data.types(self.engine).get(match, '[?]'))
            tprev = tablename
        print

    def what_references(self, arg):
        """Show fields referencing the input table/field arg.

        If arg is a tablename, then print fields which reference
        any field in tablename. If arg is a field (specified by 
        tablename.fieldname), then print only fields which reference 
        the specified table.field. 

        Args:
            arg: Either a table name or a [table.field] name"""
        if not self.connected:
            print self.not_connected_message
            return
        bits = arg.split('.', 1)
        tablename = bits[0]
        fieldname = bits[1] if len(bits) > 1 else ''
        field = table = None
        meta = self.completion_data.sa_metadata
        meta.reflect() # XXX: can be very slow! TODO: don't do this
        for tname, tbl in meta.tables.iteritems():
            if tbl.name.lower() == tablename.lower():
                table = tbl
                break
        if table is None:
            print "Could not find table `%s`" % (tablename,)
            return
        if fieldname:
            for col in table.columns:
                if col.name == fieldname:
                    field = col
                    break
        if fieldname and field is None:
            print "Could not find `%s.%s`" % (tablename, fieldname)
            return
        refs = []
        for tname, tbl in meta.tables.iteritems():
            for fk in tbl.foreign_keys:
                if (field is not None and fk.references(table) \
                        and bool(fk.get_referent(table) == field)) \
                        or (field is None and fk.references(table)):
                    refs.append(("%s.%s" % (fk.parent.table.name, 
                                           fk.parent.name),
                                fk.target_fullname))
        if refs:
            maxleft = max(map(lambda x: len(x[0]), refs)) + 2
            fmt = "%%-%ss references %%s" % (maxleft,)
        for ref in sorted(refs, key=lambda x: x[0]):
            print fmt % ref

    def render_result(self, cursor):
        """Render a result set and pipe through less.

        Args:
            cursor: iterable of tuples, with one special method: 
                    cursor.keys() which returns a list of string columns
                    headings for the tuples.
        """
        try:
            out = os.popen('less -FXRiS','w') ## XXX: use ipython's pager abstraction
            if self.sqlformat == 'csv':
                self.format_result_csv(cursor, out=out)
            else:
                self.format_result_pretty(cursor, out=out)
        except IOError, msg:
            if msg.args == (32, 'Broken pipe'): # user quit
                pass
            else:
                raise
        finally:
            out.close()

    def format_result_pretty(self, cursor, out=sys.stdout):
        """Render an SQL result set as an ascii-table.

        Renders an SQL result set to `out`, some file-like object. 
        Assumes that we can determine the current terminal height and 
        width via the termsize module.

        Args:
            cursor: cursor-like object. See: render_result()
            out: file-like object.

        """
        cols, lines = termsize()
        headings = cursor.keys()
        for screenrows in isublists(cursor, lines - 4):
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

    def format_result_csv(self, cursor, out=sys.stdout):
        """Render an sql result set in CSV format.

        Args:
            result: cursor-like object: see render_result()
            out: file-like object to write results to.
        """
        writer = csv.writer(out)
        writer.writerow(cursor.keys())
        writer.writerows(cursor)

    def interested_in(self, completer, text, line_buffer=None):
        """Return True if ipydb should try to do completions on the current line_buffer
        otherwise return False. 

        Args:
            completer: IPython.core.completer.IPCompleter instance.
            text: Current token (str) of text being completed. 
            line_buffer: str text for the whole line. 
        Returns:
            True if ipydb should try to complete text, False otherwise. 
        """
        completion_magics = "what_references show_fields connect " \
                            "sql select insert update delete sqlformat".split()
        if text and not line_buffer:
            return True # this is unfortunate...
        else:
            first_token = line_buffer.split()[0].lstrip('%')
            if first_token in completion_magics:
                return True
            magic_assignment_re = r'^\s*\S+\s*=\s*%({magics})'.format(magics='|'.join(completion_magics))
            return re.match(magic_assignment_re, line_buffer) is not None
            

    def complete(self, completer, text, line_buffer=None):
        """Return a list of "tab-completion" strings for text.

        Args:
            completer: IPython.core.completer.IPCompleter instance.
            text: String of text to complete.
            line_buffer: Full line of text (str) that is being completed. 
        Returns:
            list of strings which can complete the input text.
        """
        matches = []
        matches_append = matches.append
        if not self.interested_in(completer, text, line_buffer):
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
        """Helper to substring-match text in a list-of-lists.

        Args:
            lists: a list of lists of strings.
            text: text to substring match against lists.
            appendfunc: callable, called with each string from
                        and of the input lists that can complete
                        text - appendfunc(match)
        """
        n = len(text)
        for word in itertools.chain(*lists):
            if word[:n] == text:
                appendfunc(word)

    def complete_sql(self, completer, text, line_buffer=None, first_token=None):
        """Return completion suggestions based up database schema terms.

        See complete() for keyword arguments. 

        Args:
            first_token: The first non-whitespace token from the front
                         of line_buffer. 
        Returns:
            A List of strings which can complete input text. 
        """
        if not self.connected:
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
        self.match_lists([tables, fields, RESERVED_WORDS], 
                        text, matches_append)
        return matches

