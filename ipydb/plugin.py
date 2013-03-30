# -*- coding: utf-8 -*-

"""
The ipydb plugin.

:copyright: (c) 2012 by Jay Sweeney.
:license: see LICENSE for more details.
"""
from ConfigParser import DuplicateSectionError
import csv
import fnmatch
import itertools
import os
import sys

from IPython.core.plugin import Plugin
from metadata import CompletionDataAccessor
import sqlalchemy as sa
from utils import multi_choice_prompt

import asciitable
from asciitable import FakedResult, PivotResultSet
from completion import IpydbCompleter, ipydb_complete, reassignment
import engine
from magic import SqlMagics, register_sql_aliases


class Pager(object):
    def __init__(self):
        self.out = os.popen('less -FXRiS', 'w')  # XXX: use ipython's pager

    def __enter__(self):
        return self.out

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type == IOError and exc_val and \
                exc_val.args == (32, 'Broken pipe'):
            return True  # user quit pager
        self.out.close()


class SqlPlugin(Plugin):
    """The ipydb plugin - manipulate databases from ipython."""

    max_fieldsize = 100  # configurable?
    completion_accessor = CompletionDataAccessor()
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
        return ' !' if self.completion_accessor.reflecting(self.engine) else ''

    def safe_url(self, url_string):
        """Return url_string with password removed."""
        url = None
        try:
            url = sa.engine.url.make_url(str(url_string))
            url.password = 'xxx'
        except:
            pass
        return url

    @property
    def comp_data(self):
        """Returns completion data for the currect connection.
        Returns:
            Instance of ipydb.metadata.MetaData().
        """
        if not self.connected:
            return None
        return self.completion_accessor.get_metadata(self.engine)

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
                print "Save aborted"
                return
        print "`%s` saved to ~/.db-connections" % (configname,)

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
            self.completion_accessor.get_metadata(self.engine, noisy=True)
        return True

    def flush_metadata(self):
        """Delete cached schema information"""
        print "Deleting metadata..."
        self.completion_accessor.flush()
        if self.connected:
            self.completion_accessor.get_metadata(self.engine)

    def execute(self, query, params=None, multiparams=None):
        """Execute query against current db connection, return result set.

        Args:
            query: String query to execute.
            args: Dictionary of bind parameters for the query.
            multiargs: Collection/iterable of dictionaries of bind parameters.
        Returns:
            Sqlalchemy's DB-API cursor-like object.
        """
        result = None
        if params is None:
            params = {}
        if multiparams is None:
            multiparams = []
        if not self.connected:
            print self.not_connected_message
        else:
            bits = query.split()
            if len(bits) == 2 and bits[0].lower() == 'select' and \
                    bits[1] in self.comp_data.tables:
                query = 'select * from %s' % bits[1]
            elif bits[0].lower() in 'insert update delete'.split() \
                    and not self.trans_ctx and not self.autocommit:
                self.begin()
            conn = self.engine
            if self.trans_ctx and self.trans_ctx.transaction.is_active:
                conn = self.trans_ctx.conn
            try:
                result = conn.execute(query, *multiparams, **params)
            except Exception, e:
                if self.debug:
                    raise
                print e.message
        return result

    def run_sql_script(self, script, interactive=False, delimiter='/'):
        """Run all SQL statments found in a text file.

        Args:
            script: path to file containing SQL statments.
            interactive: run in ineractive mode, showing and prompting each
                         statement. default: False.
            delimiter: SQL statement delimiter, must be on a new line
                       by itself. default: '/'.
        """
        if not self.connected:
            print self.not_connected_message
            return
        with open(script) as fin:
            current = ''
            while True:
                line = fin.readline()
                if line.strip() == delimiter or (line == '' and current):
                    if interactive:
                        print current
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
                        self.execute(current)
                        current = ''
                else:
                    current += line
                if line == '':
                    break

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
        tablenames = self.comp_data.tables
        if not globs:
            matches = tablenames
        else:
            for glob in globs:
                matches.update(fnmatch.filter(tablenames, glob))
        matches = sorted(matches)
        self.render_result(FakedResult(((r,) for r in matches), ['Table']))
        # print '\n'.join(sorted(matches))

    def describe(self, table):
        """Print information about a table."""
        if not self.connected:
            print self.not_connected_message
            return
        #self.show_fields(table)
        if table not in self.comp_data.tables:
            print "Table not found: %s" % table
            return
        pkhash = {pk.table: pk.columns for pk in self.comp_data.primary_keys}
        with self.pager() as out:
            items = []
            for col in self.comp_data.get_fields(table):
                type_ = self.comp_data.types.get('%s.%s' % (table, col), '???')
                if table in pkhash and col in pkhash[table]:  # tag primary key
                    col = '*%s' % col
                items.append((col, type_))
            items.sort()
            asciitable.draw(
                FakedResult([items], 'Field Type'.split()),
                out, paginate=False,
                max_fieldsize=self.max_fieldsize)
            out.write('Primary Key (*)\n')
            out.write('---------------\n')
            pk = self.comp_data.get_primarykey(table)
            cols = pk.columns if pk else []
            out.write('  ')
            if not pk:
                out.write('(None Found!)')
            out.write(', '.join(cols))
            out.write('\n\n')
            out.write('Foreign Keys\n')
            out.write('------------\n')
            fks = self.comp_data.get_foreignkeys(table)
            fk = None
            for fk in fks:
                out.write('  %s\n' % str(fk))
            if fk is None:
                out.write('  (None Found)')

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
        dottedfields = self.comp_data.dottedfields
        pkhash = {pk.table: pk.columns for pk in self.comp_data.primary_keys}
        if not globs:
            matches = dottedfields
        for glob in globs:
            bits = glob.split('.', 1)
            if len(bits) == 1:  # table name only
                glob += '.*'
            matches.update(fnmatch.filter(dottedfields, glob))
        tprev = None
        with self.pager() as out:
            for match in sorted(matches):
                tablename, fieldname = match.split('.', 1)
                if tablename in pkhash and fieldname in pkhash[tablename]:
                    fieldname = '*%s' % fieldname
                if tablename != tprev:
                    if tprev is not None:
                        out.write("\n")
                    out.write(tablename + '\n')
                    out.write('-' * len(tablename) + '\n')
                out.write("    %-35s%s\n" % (
                    fieldname,
                    self.comp_data.types.get(match, '[?]')))
                tprev = tablename
            out.write('\n')

    def show_joins(self, table):
        """Show all incoming and outgoing joins possible for a table.
        Args:
            table: Table name.
        """
        if not self.connected:
            print self.not_connected_message
            return
        fks = self.comp_data.get_all_joins(table)
        for fk in fks:
            print fk.as_join()

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
        fks = self.comp_data.fields_referencing(
            tablename, fieldname)
        for fk in fks:
            print fk

    def show_fks(self, table):
        """Show foreign keys for the given table

        Args:
            table: A table name."""
        if not self.connected:
            print self.not_connected_message
            return
        fks = self.comp_data.get_foreignkeys(table)
        for fk in fks:
            print fk

    def pager(self):
        return Pager()

    def render_result(self, cursor, paginate=True, filepath=None):
        """Render a result set and pipe through less.

        Args:
            cursor: iterable of tuples, with one special method:
                    cursor.keys() which returns a list of string columns
                    headings for the tuples.
        """
        if filepath:
            out = open(filepath, 'w')
        else:
            out = self.pager()
        with out as out:  # i'm being lazy with Pager()
            if self.sqlformat == 'csv':
                self.format_result_csv(cursor, out=out)
            else:
                asciitable.draw(cursor, out=out,
                                paginate=paginate,
                                max_fieldsize=self.max_fieldsize)

    def format_result_csv(self, cursor, out=sys.stdout):
        """Render an sql result set in CSV format.

        Args:
            result: cursor-like object: see render_result()
            out: file-like object to write results to.
        """
        writer = csv.writer(out)
        writer.writerow(cursor.keys())
        writer.writerows(cursor)
