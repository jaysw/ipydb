# -*- coding: utf-8 -*-

"""
The ipydb plugin.

:copyright: (c) 2012 by Jay Sweeney.
:license: see LICENSE for more details.
"""
from __future__ import print_function
from configparser import DuplicateSectionError
import fnmatch
import functools
import logging
import os
import shlex
import subprocess
import sys

try:
    from traitlets.config.configurable import Configurable
except ImportError:
    # IPython 3 support
    from IPython.config.configurable import Configurable

from future.utils import viewvalues
import sqlalchemy as sa

from ipydb.utils import multi_choice_prompt, UnicodeWriter
from ipydb.metadata import MetaDataAccessor
from ipydb import asciitable
from ipydb.asciitable import FakedResult
from ipydb.completion import IpydbCompleter, ipydb_complete, reassignment
from ipydb import engine
from ipydb.magic import SqlMagics, register_sql_aliases
from ipydb.metadata import model

# pandas as a extra requirement
_has_pandas = False

try:
    import pandas as pd
    _has_pandas = True
except ImportError:
    pass

log = logging.getLogger(__name__)

SQLFORMATS = ['csv', 'table']

os.environ['PYTHONIOENCODING'] = 'utf-8'


def connected(f):
    """Decorator - bail if not connected"""
    @functools.wraps(f)
    def wrapper(plugin, *args, **kw):
        if not plugin.connected:
            print(plugin.not_connected_message)
            return
        return f(plugin, *args, **kw)
    return wrapper


class Popen(subprocess.Popen):

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.stdout:
            self.stdout.close()
        if self.stderr:
            self.stderr.close()
        if self.stdin:
            self.stdin.close()
        # Wait for the process to terminate, to avoid zombies.
        self.wait()

    def write(self, bytestring):
        self.stdin.write(bytestring)
        #self.communicate(input=bytestring, timeout=10)


def pager():
    return Popen(shlex.split('less -FXRiS'), stdin=subprocess.PIPE)


class SqlPlugin(Configurable):
    """The ipydb plugin - manipulate databases from ipython."""

    max_fieldsize = 100  # configurable?
    metadata_accessor = MetaDataAccessor()
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
        self.autocommit = False
        self.trans_ctx = None
        self.debug = False
        self.show_sql = False
        default, configs = engine.getconfigs()
        self.init_completer()
        if default:
            self.connect(default)

    def init_completer(self):
        """Setup ipydb sql completion."""
        # to complete things like table.* we need to
        # change the ipydb spliter delims:
        delims = self.shell.Completer.splitter.delims.replace('*', '')
        self.shell.Completer.splitter.delim = delims
        if self.shell.Completer.readline:
            self.shell.Completer.readline.set_completer_delims(delims)
        self.completer = IpydbCompleter(self.get_metadata)
        for str_key in self.completer.commands_completers.keys():
            str_key = '%' + str_key  # as ipython magic commands
            self.shell.set_hook('complete_command', ipydb_complete,
                                str_key=str_key)
        # add a regex dispatch for assignments: res = %select -r ...
        self.shell.set_hook('complete_command',
                            ipydb_complete, re_key=reassignment)

    def set_debug(self, debug):
        self.debug = debug
        self.metadata_accessor.debug = debug

    @connected
    def get_engine(self):
        """Returns current sqlalchemy engine reference, if there was one."""
        return self.engine

    def get_db_ps1(self, *args, **kwargs):
        """ Return current host/db for use in ipython's prompt PS1. """
        if not self.connected:
            return ''
        if self.nickname:
            return " " + self.nickname
        host = self.engine.url.host or ''

        if '.' in host:
            host = host.split('.')[0]
        host = host[:15]  # don't like long hostnames
        db = '?'
        if self.engine.url.database:
            db = self.engine.url.database[:15]
        if host:
            url = "%s/%s" % (host, db)
        else:
            url = db
        return " " + url

    def get_transaction_ps1(self, *args, **kw):
        """Return '*' if ipydb has an active transaction."""
        if not self.connected:
            return ''
        # I want this: ⚡
        # but looks like IPython is expecting ascii for the PS1.
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
        return ' !' if self.metadata_accessor.reflecting(self.engine) else ''

    def safe_url(self, url_string):
        """Return url_string with password removed."""
        url = None
        try:
            url = sa.engine.url.make_url(str(url_string))
            url.password = 'xxx'
        except:  # pragma: no cover
            pass
        return url

    def get_metadata(self):
        """Returns database metadata for the currect connection.
        Returns:
            Instance of ipydb.metadata.Database().
        """
        if not self.connected:
            return model.Database()
        return self.metadata_accessor.get_metadata(
            self.engine, do_reflection=self.do_reflection)

    def save_connection(self, configname):
        """Save the current connection to ~/.db-connections."""
        try:
            engine.save_connection(configname, self.engine)
        except DuplicateSectionError:
            over = self.shell.ask_yes_no(
                '`%s` exists, Overwrite (y/n)?' % configname)
            if over:
                engine.save_connection(
                    configname, self.engine, overwrite=True)
            else:
                print("Save aborted")
                return
        print("`%s` saved to ~/.db-connections" % (configname,))

    def connect(self, configname=None):
        """Connect to a database based upon its `nickname`.

        See ipydb.magic.connect() for details.
        """
        default, configs = engine.getconfigs()
        success = False

        def available():
            print(self.connect.__doc__)
            print("Available connection nicknames: %s" % (
                ' '.join(sorted(configs.keys()))))
        if not configname:
            available()
        elif configname not in configs:
            print("Config `%s` not found. " % configname)
            available()
        else:
            config = configs[configname]
            connect_args = {}
            success = self.connect_url(
                engine.make_connection_url(config), connect_args)
            if success:
                self.nickname = configname
        return success

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
            print("You have an active transaction, either %commit or "
                  "%rollback before connecting to a new database.")
            return False
        try:
            parsed_url = sa.engine.url.make_url(str(url))
        except sa.exc.ArgumentError as e:
            print(e)
            return False
        safe_url = self.safe_url(parsed_url)
        if safe_url:
            print("ipydb is connecting to: %s" % safe_url)
        try:
            self.engine = engine.from_url(parsed_url,
                                          connect_args=connect_args)
        except ImportError:  # pragma: nocover
            print("It looks like you don't have a driver for %s.\n"
                  "See the following URL for supported "
                  "database drivers:\n\t%s" % (
                    parsed_url.drivername,
                    'http://docs.sqlalchemy.org/en/latest/'
                    'dialects/index.html#included-dialects'))
            return False
        # force a connect so that we can fail early if the connection url won't
        # work
        try:
            with self.engine.connect():
                pass
        except sa.exc.OperationalError as e:  # pragma: nocover
            print(e)
            return False

        self.connected = True
        self.nickname = None
        if self.do_reflection:
            self.metadata_accessor.get_metadata(self.engine, noisy=True)
        return True

    @connected
    def flush_metadata(self):
        """Delete cached schema information"""
        print("Deleting metadata...")
        if self.do_reflection:
            self.metadata_accessor.flush(self.engine)
            self.metadata_accessor.get_metadata(self.engine, noisy=True)

    @connected
    def execute(self, query, params=None, multiparams=None):
        """Execute query against current db connection, return result set.

        Args:
            query: String query to execute.
            args: Dictionary of bind parameters for the query.
            multiargs: Collection/iterable of dictionaries of bind parameters.
        Returns:
            Sqlalchemy's DB-API cursor-like object.
        """
        rereflect = False
        ddl_commands = 'create drop alter truncate rename'.split()
        want_tx = 'insert update delete merge replace'.split()
        result = None
        if params is None:
            params = {}
        if multiparams is None:
            multiparams = []
        bits = query.split()
        if (len(bits) == 2 and bits[0].lower() == 'select' and
                bits[1] in self.get_metadata().tables):
            query = 'select * from %s' % bits[1]
        elif (bits[0].lower() in want_tx and
              not self.trans_ctx and not self.autocommit):
            self.begin()  # create tx before doing modifications
        elif bits[0].lower() in ddl_commands:
            rereflect = True
        conn = self.engine
        if self.trans_ctx and self.trans_ctx.transaction.is_active:
            conn = self.trans_ctx.conn
        try:
            result = conn.execute(query, *multiparams, **params)
            if rereflect and self.do_reflection:  # schema changed
                self.metadata_accessor.get_metadata(self.engine,
                                                    force=True, noisy=True)
        except Exception as e:  # pragma: nocover
            if self.debug:
                raise
            print(e.message)
        return result

    @connected
    def run_sql_script(self, script, interactive=False, delimiter='/'):
        """Run all SQL statments found in a text file.

        Args:
            script: path to file containing SQL statments.
            interactive: run in ineractive mode, showing and prompting each
                         statement. default: False.
            delimiter: SQL statement delimiter, must be on a new line
                       by itself. default: '/'.
        """
        with open(script) as fin:
            current = ''
            while True:
                line = fin.readline()
                if line.strip() == delimiter or (line == '' and current):
                    if interactive:
                        print(current)
                        choice = multi_choice_prompt(
                            'Run this statement '
                            '([y]es, [n]o, [a]ll, [q]uit):',
                            {'y': 'y', 'n': 'n', 'a': 'a', 'q': 'q'})
                        if choice == 'y':
                            pass
                        elif choice == 'n':
                            current = ''
                        elif choice == 'a':
                            interactive = False
                        elif choice == 'q':
                            break
                    if current:
                        if current.strip().lower() == 'commit':
                            self.commit()
                        elif current.strip().lower() == 'rollback':
                            self.rollback()
                        else:
                            self.execute(current)
                        current = ''
                else:
                    current += line
                if line == '':
                    break

    @connected
    def begin(self):
        """Start a new transaction against the current db connection."""
        if not self.trans_ctx or not self.trans_ctx.transaction.is_active:
            self.trans_ctx = self.engine.begin()
        else:
            print("You are already in a transaction"
                  " block and nesting is not supported")

    @connected
    def commit(self):
        """Commit current transaction if there was one."""
        if self.trans_ctx:
            with self.trans_ctx:
                pass
            self.trans_ctx = None
        else:
            print("No active transaction")

    @connected
    def rollback(self):
        """Rollback current transaction if there was one."""
        if self.trans_ctx:
            self.trans_ctx.transaction.rollback()
            self.trans_ctx = None
        else:
            print("No active transaction")

    @connected
    def show_tables(self, *globs, **kw):
        """Print a list of tablenames matching input glob/s.

        All table names are printed if no glob is given, otherwise
        just those table names matching any of the *globs are printed.

        Args:
            *glob: zero or more globs to match against table names.
        Kwargs:
            views: (bool) show views only

        """
        matches = set()
        if kw.get('views'):
            tablenames = [v.name for v in self.get_metadata().views]
        else:
            tablenames = self.get_metadata().tables
        if not globs:
            matches = tablenames
        else:
            for glob in globs:
                matches.update(fnmatch.filter(tablenames, glob))
        matches = sorted(matches)
        self.render_result(FakedResult(((r,) for r in matches), ['Table']))
        # print '\n'.join(sorted(matches))

    @connected
    def describe(self, table):
        """Print information about a table."""
        if table not in self.get_metadata().tables:
            print("Table not found: %s" % table)
            return
        tbl = self.get_metadata().tables[table]

        def nullstr(nullable):
            return 'NULL' if nullable else 'NOT NULL'

        def namestr(c):
            return ('*%s' if c.primary_key else '%s') % c.name

        with pager() as out:
            items = ((namestr(c), c.type, nullstr(c.nullable))
                     for c in tbl.columns)
            out.write(b'Columns' + b'\n')
            asciitable.draw(
                FakedResult(sorted(items), 'Name Type Nullable'.split()),
                out, paginate=True,
                max_fieldsize=5000)
            out.write(b'\n')
            out.write(b'Primary Key (*)\n')
            out.write(b'---------------\n')
            pk = ', '.join(c.name for c in tbl.columns if c.primary_key)
            out.write(b'  ')
            if not pk:
                out.write(b'(None Found!)')
            else:
                out.write(pk.encode('utf8'))
            out.write(b'\n\n')
            out.write(b'Foreign Keys\n')
            out.write(b'------------\n')
            fks = self.get_metadata().foreign_keys(table)
            fk = None
            for fk in fks:
                out.write(('  %s\n' % str(fk)).encode('utf8'))
            if fk is None:
                out.write(b'  (None Found)')
            out.write(('\n\nReferences to %s\n' % table).encode('utf8'))
            out.write(b'--------------' + b'-' * len(table) + b'\n')
            fks = self.get_metadata().fields_referencing(table)
            fk = None
            for fk in fks:
                out.write(b'  ' + str(fk).encode('utf8') + b'\n')
            if fk is None:
                out.write(b'  (None found)\n')
            out.write(b'\n\nIndexes\n')

            def items():
                for idx in self.get_metadata().indexes(table):
                    yield (idx.name, ', '.join(c.name for c in idx.columns),
                           idx.unique)
            asciitable.draw(
                FakedResult(sorted(items()), 'Name Columns Unique'.split()),
                out, paginate=True, max_fieldsize=5000)

    @connected
    def show_fields(self, *globs):
        """
        Print a list of fields matching the input glob tableglob[.fieldglob].

        See ipydb.magic.show_fields for examples.

        Args:
            *globs: list of [tableglob].[fieldglob] strings
        """

        def starname(col):
            star = '*' if col.primary_key else ''
            return star + col.name

        def glob_columns(table):
            for c in table.columns:
                for glob in globs:
                    bits = glob.split('.', 1)
                    if len(bits) == 1:
                        glob += '.*'
                    if fnmatch.fnmatch('%s.%s' % (table.name, c.name), glob):
                        yield c

        with pager() as out:
            for table in viewvalues(self.get_metadata().tables):
                if globs:
                    columns = list(glob_columns(table))
                else:
                    columns = table.columns
                columns = {starname(c): c for c in columns}
                if columns:
                    out.write(table.name.encode('utf8') + b'\n')
                    out.write(b'-' * len(table.name) + b'\n')
                for starcol in sorted(columns):
                    col = columns[starcol]
                    output = "    %-35s%s %s\n" % (
                        starcol,
                        col.type,
                        'NULL' if col.nullable else 'NOT NULL')
                    out.write(output.encode('utf8'))
                if columns:
                    out.write(b'\n')

    @connected
    def show_joins(self, table):
        """Show all incoming and outgoing joins possible for a table.
        Args:
            table: Table name.
        """
        with pager() as out:
            for fk in self.get_metadata().foreign_keys(table):
                out.write(fk.as_join(reverse=True).encode('utf8') + b'\n')
            for fk in self.get_metadata().fields_referencing(table):
                out.write(fk.as_join().encode('utf8') + b'\n')

    @connected
    def what_references(self, arg):
        """Show fields referencing the input table/field arg.

        If arg is a tablename, then print fields which reference
        any field in tablename. If arg is a field (specified by
        tablename.fieldname), then print only fields which reference
        the specified table.field.

        Args:
            arg: Either a table name or a [table.field] name"""
        with pager() as out:
            bits = arg.split('.', 1)
            tablename = bits[0]
            fieldname = bits[1] if len(bits) > 1 else None
            fks = self.get_metadata().fields_referencing(tablename, fieldname)
            for fk in fks:
                out.write(str(fk).encode('utf8') + b'\n')

    @connected
    def show_fks(self, table):
        """Show foreign keys for the given table

        Args:
            table: A table name."""
        with pager() as out:
            fks = self.get_metadata().foreign_keys(table)
            for fk in fks:
                out.write(str(fk).encode('utf8') + b'\n')

    def render_result(self, cursor, paginate=True,
                      filepath=None, sqlformat=None):
        """Render a result set and pipe through less.

        Args:
            cursor: iterable of tuples, with one special method:
                    cursor.keys() which returns a list of string columns
                    headings for the tuples.
        """
        if not sqlformat:
            sqlformat = self.sqlformat
        if filepath:
            out = open(filepath, 'w')
            sqlformat = 'csv'
        else:
            out = pager()
        with out as stdout:
            if sqlformat == 'csv':
                self.format_result_csv(cursor, out=stdout)
            else:
                asciitable.draw(cursor, out=stdout,
                                paginate=paginate,
                                max_fieldsize=self.max_fieldsize)

    def format_result_csv(self, cursor, out=sys.stdout):
        """Render an sql cursor set in CSV format.

        Args:
            cursor: cursor-like object: see render_result()
            out: file-like object to write results to.
        """
        writer = UnicodeWriter(out)
        writer.writerow(cursor.keys())
        writer.writerows(cursor)

    def build_dataframe(self, cursor):
        """Reture an sql result set in pandas DataFrame format.

        Args:
            cursor: a sqlalchemy connection cursor
        """
        if not _has_pandas:
            print("Warning: Pandas support not installed."
                  "Please use `pip install 'ipydb[notebook]'` "
                  "to add support for pandas dataframes in ipydb.")
            return None

        data = cursor.fetchall()  # XXX: clamp upper limit on fetch here
        columns = cursor.keys()
        frame = pd.DataFrame.from_records(data, columns=columns)
        return frame
