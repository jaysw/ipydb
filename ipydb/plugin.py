# -*- coding: utf-8 -*-

"""
The ipydb plugin.

:copyright: (c) 2012 by Jay Sweeney.
:license: see LICENSE for more details.
"""
import csv
import fnmatch
import itertools
import os
import sys

from IPython.core.plugin import Plugin
from metadata import CompletionDataAccessor
import sqlalchemy as sa
from termsize import termsize

from completion import IpydbCompleter, ipydb_complete, reassignment
import engine
from magic import SqlMagics, register_sql_aliases


def sublists(l, n):
    return (l[i:i + n] for i in range(0, len(l), n))


def isublists(l, n):
    return itertools.izip_longest(*[iter(l)] * n)


class FakedResult(object):

    def __init__(self, items, headings):
        self.items = items
        self.headings = headings

    def __iter__(self):
        return iter(self.items)

    def keys(self):
        return self.headings


class PivotResultSet(object):
    def __init__(self, rs):
        self.rs = rs

    def __iter__(self):
        return (r.items() for r in self.rs)

    def keys(self):
        return ['Field', 'Value']


class SqlPlugin(Plugin):
    """The ipydb plugin - manipulate databases from ipython."""

    max_fieldsize = 100  # configurable?
    completion_data = CompletionDataAccessor()
    sqlformats = "table csv".split()
    not_connected_message = "ipydb is not connected to a database. " \
        "Try:\n\t%connect CONFIGNAME\nor try:\n\t" \
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
        register_sql_aliases(shell.magics_manager, self.auto_magics)
        self.sqlformat = 'table'  # 'table' | 'csv'
        self.do_reflection = True
        self.connected = False
        self.engine = None
        self.nickname = None
        self.autocommit = True
        self.trans_ctx = None
        self.debug = False
        default, configs = engine.getconfigs()
        self.init_completer()
        if default:
            self.connect(default)

    def init_completer(self):
        """Setup ipydb sql completion."""
        # to complete things like table.* we needto
        # change the ipydb spliiter delims:
        delims = self.shell.Completer.splitter.delims.replace('*', '')
        self.shell.Completer.splitter.delim = delims
        if self.shell.Completer.readline:
            self.shell.Completer.readline.set_completer_delims(delims)
        self.completer = IpydbCompleter(self)
        for str_key in self.completer.commands_completers.keys():
            str_key = '%' + str_key  # as ipython magic commands
            self.shell.set_hook('complete_command',
                                ipydb_complete,
                                str_key=str_key)
        # add a regex dispatch for assignments: res = %select -r ...
        self.shell.set_hook('complete_command',
                            ipydb_complete, re_key=reassignment)

    def get_engine(self):
        """Returns current sqlalchemy engine reference, if there was one."""
        if not self.connected:
            print self.not_connected_message
        return self.engine

    def get_db_ps1(self, *args, **kwargs):
        """ Return current host/db for use in ipython's prompt PS1. """
        if not self.connected:
            return ''
        if self.nickname:
            return " " + self.nickname
        host = self.engine.url.host
        if '.' in host:
            host = host.split('.')[0]
        host = host[:15]  # don't like long hostnames
        db = '?'
        if self.engine.url.database:
            db = self.engine.url.database[:15]
        url = "%s/%s" % (host, db)
        return " " + url

    def get_transaction_ps1(self, *args, **kw):
        """Return '*' if ipydb has an active transaction."""
        if not self.connected:
            return ''
        # I want this: ⚡
        # but looks like IPython is expecting ascii for the PS1!?
        if self.trans_ctx and self.trans_ctx.transaction.is_active:
            return ' *'
        else:
            return ''

    def get_reflecting_ps1(self, *args, **kw):
        """
        Return a string indictor if background schema reflection is running.
        """
        if not self.connected:
            return ''
        return ' !' if self.completion_data.reflecting(self.engine) else ''

    def safe_url(self, url_string):
        """Return url_string with password removed."""
        url = None
        try:
            url = sa.engine.url.make_url(str(url_string))
            url.password = 'xxx'
        except:
            pass
        return url

    def get_completion_data(self):
        """return completion data information for current connection"""
        if not self.connected:
            return self.completion_data._meta()
        else:
            return self.completion_data.get_metadata(self.engine)

    @property
    def metadata(self):
        """Get sqlalchemy.MetaData instance for current connection."""
        if not self.connected:
            return None
        meta = getattr(self, '_metadata', None)
        if meta is None or self._metadata.bind != self.engine:
            self._metadata = sa.MetaData(bind=self.engine)
        return self._metadata

    def save_connection(self, configname):
        """Save the current connection to ~/.db-connections."""
        engine.save_connection(configname, self.engine)

    def connect(self, configname=None):
        """Connect to a database based upon its `nickname`.

        See ipydb.magic.connect() for details.
        """
        default, configs = engine.getconfigs()

        def available():
            print self.connect.__doc__
            print "Available connection nicknames: %s" % (
                ' '.join(sorted(configs.keys())))
        if not configname:
            available()
        elif configname not in configs:
            print "Config `%s` not found. " % configname
            available()
        else:
            config = configs[configname]
            connect_args = {}
            self.connect_url(engine.make_connection_url(config), connect_args)
            self.nickname = configname
        return self.connected

    def connect_url(self, url, connect_args={}):
        """Connect to a database using an SqlAlchemy URL.

        Args:
            url: An SqlAlchemy-style DB connection URL.
            connect_args: extra argument to be passed to the underlying
                          DB-API driver.
        Returns:
            True if connection was successful.
        """
        if self.trans_ctx and self.trans_ctx.transaction.is_active:
            print "You have an active transaction, either %commit or " \
                "%rollback before connecting to a new database."
            return
        safe_url = self.safe_url(url)
        if safe_url:
            print "ipydb is connecting to: %s" % safe_url
        self.engine = engine.from_url(url, connect_args=connect_args)
        self.connected = True
        self.nickname = None
        if self.do_reflection:
            self.completion_data.get_metadata(self.engine)
        return True

    def flush_metadata(self):
        """Delete cached schema information"""
        print "Deleting metadata..."
        self.completion_data.flush()
        if self.connected:
            self.completion_data.get_metadata(self.engine)

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
            bits = query.split()
            if len(bits) == 2 and bits[0].lower() == 'select' and \
                    bits[1] in self.completion_data.tables(self.engine):
                query = 'select * from %s' % bits[1]
            conn = self.engine
            if self.trans_ctx and self.trans_ctx.transaction.is_active:
                conn = self.trans_ctx.conn.execute
            try:
                result = conn.execute(query)
            except Exception, e:
                if self.debug:
                    raise
                print e.message
        return result

    def begin(self):
        """Start a new transaction against the current db connection."""
        if not self.connected:
            print self.not_connected_message
            return
        if not self.trans_ctx or not self.trans_ctx.transaction.is_active:
            self.trans_ctx = self.engine.begin()
        else:
            print "You are already in a transaction" \
                " block and nesting is not supported"

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
        self.render_result(FakedResult(((r,) for r in matches), ['Table']))
        # print '\n'.join(sorted(matches))

    def show_fields(self, *globs):
        """
        Print a list of fields matching the input glob tableglob[.fieldglob].

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
            if len(bits) == 1:  # table name only
                glob += '.*'
            matches.update(fnmatch.filter(dottedfields, glob))
        tprev = None
        try:
            out = self.get_pager()
            for match in sorted(matches):
                tablename, fieldname = match.split('.', 1)
                if tablename != tprev:
                    if tprev is not None:
                        out.write("\n")
                    out.write(tablename + '\n')
                    out.write('-' * len(tablename) + '\n')
                out.write("    %-35s%s\n" % (
                    fieldname,
                    self.completion_data.types(self.engine).get(match, '[?]')))
                tprev = tablename
            out.write('\n')
        except IOError, msg:
            if msg.args == (32, 'Broken pipe'):  # user quit
                pass
            else:
                raise
        finally:
            out.close()

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
        meta.reflect()  # XXX: can be very slow! TODO: don't do this
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
                if ((field is not None and fk.references(table) and
                        bool(fk.get_referent(table) == field)) or
                        (field is None and fk.references(table))):
                    sourcefield = "%s.%s" % (
                        fk.parent.table.name, fk.parent.name)
                    refs.append((sourcefield, fk.target_fullname))
        if refs:
            maxleft = max(map(lambda x: len(x[0]), refs)) + 2
            fmt = u"%%-%ss references %%s" % (maxleft,)
        for ref in sorted(refs, key=lambda x: x[0]):
            print fmt % ref

    def get_pager(self):
        return os.popen('less -FXRiS', 'w')  # XXX: use ipython's pager

    def render_result(self, cursor, paginate=True):
        """Render a result set and pipe through less.

        Args:
            cursor: iterable of tuples, with one special method:
                    cursor.keys() which returns a list of string columns
                    headings for the tuples.
        """
        try:
            out = self.get_pager()
            if self.sqlformat == 'csv':
                self.format_result_csv(cursor, out=out)
            else:
                self.format_result_pretty(cursor, out=out,
                                          paginate=paginate)
        except IOError, msg:
            if msg.args == (32, 'Broken pipe'):  # user quit
                pass
            else:
                raise
        finally:
            out.close()

    def format_result_pretty(self, cursor, out=sys.stdout, paginate=True):
        """Render an SQL result set as an ascii-table.

        Renders an SQL result set to `out`, some file-like object.
        Assumes that we can determine the current terminal height and
        width via the termsize module.

        Args:
            cursor: cursor-like object. See: render_result()
            out: file-like object.

        """

        def heading_line(sizes):
            for size in sizes:
                out.write('+' + '-' * (size + 2))
            out.write('+\n')

        def draw_headings(headings, sizes):
            heading_line(sizes)
            for idx, size in enumerate(sizes):
                fmt = '| %%-%is ' % size
                out.write((fmt % headings[idx]))
            out.write('|\n')
            heading_line(sizes)

        cols, lines = termsize()
        headings = cursor.keys()
        heading_sizes = map(lambda x: len(x), headings)
        if paginate:
            cursor = isublists(cursor, lines - 4)
        for screenrows in cursor:
            sizes = heading_sizes[:]
            for row in screenrows:
                if row is None:
                    break
                for idx, value in enumerate(row):
                    if not isinstance(value, basestring):
                        value = str(value)
                    size = max(sizes[idx], len(value))
                    sizes[idx] = min(size, self.max_fieldsize)
            draw_headings(headings, sizes)
            for rw in screenrows:
                if rw is None:
                    break  # from isublists impl
                for idx, size in enumerate(sizes):
                    fmt = '| %%-%is ' % size
                    value = rw[idx]
                    if not isinstance(value, basestring):
                        value = str(value)
                    if len(value) > self.max_fieldsize:
                        value = value[:self.max_fieldsize - 5] + '[...]'
                    value = value.replace('\n', '^')
                    value = value.replace('\r', '^').replace('\t', ' ')
                    out.write((fmt % value))
                out.write('|\n')
            if not paginate:
                heading_line(sizes)
                out.write('\n')

    def format_result_csv(self, cursor, out=sys.stdout):
        """Render an sql result set in CSV format.

        Args:
            result: cursor-like object: see render_result()
            out: file-like object to write results to.
        """
        writer = csv.writer(out)
        writer.writerow(cursor.keys())
        writer.writerows(cursor)
