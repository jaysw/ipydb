# -*- coding: utf-8 -*-

"""
IPython magic commands registered by ipydb

:copyright: (c) 2012 by Jay Sweeney.
:license: see LICENSE for more details.
"""
import logging

from IPython.core.magic import Magics, magics_class, \
    line_magic, line_cell_magic
from IPython.core.magic_arguments import magic_arguments, \
    argument, parse_argstring
from IPython.utils.process import arg_split
import sqlparse
from ipydb.asciitable import PivotResultSet

SQL_ALIASES = 'select insert update delete create alter drop'.split()


def create_sql_alias(alias, magic_manager, sqlmagics):
    """Returns a function which calls SqlMagics.sql

    For example, create_sql_alias('select', mm, sqlm) returns a function
    which when called like this: _sqlalias('-r -a --foo=bar * from thing')
    will rearrange options in the line and result in the following call:
        sqlmagics.sql("-r -a --foo=bar select * from thing")
    """
    def _sqlalias(line, cell=None):
        """Alias to %sql"""
        opts, args = [], []
        for chunk in arg_split(line):
            # XXX: what about math?!:
            # select -1 + 5 * something from blah;
            if chunk.startswith('-') and len(chunk.strip()) > 1:
                opts.append(chunk)
            else:
                args.append(chunk)
        line = '%s %s %s' % (' '.join(opts), alias, ' '.join(args))
        return sqlmagics.sql(line, cell)
    return _sqlalias


def register_sql_aliases(magic_manager, sqlmagics):
    """Creates and registers convenience aliases to SqlMagics.sql for
    %select, %insert, %update, ...

    Args:
        magic_manager: ipython's shell.magic_manager instance
        sqlmagics: instance of SqlMagics
    """
    for alias in SQL_ALIASES:
        magic_func = create_sql_alias(alias, magic_manager, sqlmagics)
        magic_func.func_name = alias
        magic_manager.register_function(magic_func, 'line', alias)
        magic_manager.register_function(magic_func, 'cell', alias)


@magics_class
class SqlMagics(Magics):

    def __init__(self, ipydb, *a, **kw):
        super(SqlMagics, self).__init__(*a, **kw)
        self.ipydb = ipydb

    @line_magic
    def ipydb_help(self, *args):
        """Show this help message."""
        from ipydb import ipydb_help  # XXX: recursive import problem...
        ipydb_help()

    @line_magic
    def set_reflection(self, arg):
        """Toggle schema reflection."""
        if self.ipydb.do_reflection:
            self.ipydb.do_reflection = False
        else:
            self.ipydb.do_reflection = True
        print 'Schema reflection: %s' % (
            'on' if self.ipydb.do_reflection else 'off')

    @line_magic
    def engine(self, arg):
        """Returns the current SqlAlchemy engine/connection."""
        return self.ipydb.get_engine()

    @line_magic
    def debug(self, arg):
        """Turn on debugging mode for ipydb."""
        self.ipydb.debug = True
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)

    @line_magic
    def begin(self, arg):
        """Start a transaction."""
        self.ipydb.begin()

    @line_magic
    def commit(self, arg):
        """Commit active transaction, if one exists."""
        self.ipydb.commit()

    @line_magic
    def rollback(self, arg):
        """Rollback active transaction, if one exists."""
        self.ipydb.rollback()

    @magic_arguments()
    @argument('-r', '--return', dest='ret', action='store_true',
              help='Return a resultset instead of printing the results')
    @argument('-p', '--pivot', dest='single', action='store_true',
              help='View in "single record" mode')
    @argument('-m', '--multiparams', dest='multiparams', default=None,
              help='A collection of dictionaries of bind parameters')
    @argument('-a', '--params', dest='params', default=None,
              help='A dictionary of bind parameters for the sql statement')
    @argument('-f', '--format', action='store_true',
              help='pretty-print sql statement and exit')
    @argument('-o', '--output', action='store', dest='file',
              help='Write sql output as CSV to the given file')
    @argument('sql_statement',  help='The SQL statement to run', nargs="*")
    @line_cell_magic
    def sql(self, args='', cell=None):
        """Run an sql statement against the current db connection.

        Examples:
            %sql select first_name from person where first_name like 'J%'

            Also works as a multi-line ipython command:

            %%sql
                select
                    id, name, description
                from
                    my_table
                where
                    id < 10

        Returning a result set:
            To return a database cursor, use the -r option:

            results = %sql -r select first_name from employees
            for row in results:
                do_things_with(row.first_name)

        Shortcut Aliases to %sql:
            ipydb defines some 'short-cut' aliases which call %sql.
            Aliases have been added for:

                {select, insert, update, delete, create, alter, drop}

            Using aliases, you can write 'natural' SQL statements like so:

                select * from my_table

            Which results in:

                %sql select * from my_table

        """
        args = parse_argstring(self.sql, args)
        params = None
        multiparams = None
        sql = ' '.join(args.sql_statement)
        if cell is not None:
            sql += '\n' + cell
        if args.format:
            sqlstr = sqlparse.format(sql, reindent=True)
            if args.ret:
                return sqlstr
            else:
                print "\n%s" % sqlstr
            return
        if args.params:
            params = self.shell.user_ns.get(args.params, {})
        if args.multiparams:
            multiparams = self.shell.user_ns.get(args.multiparams, [])
        result = self.ipydb.execute(sql, params=params,
                                    multiparams=multiparams)
        if args.ret:
            return result
        if result and result.returns_rows:
            if args.single:
                self.ipydb.render_result(PivotResultSet(result),
                                         paginate=False,
                                         filepath=args.file)
            else:
                self.ipydb.render_result(result,
                                         paginate=not bool(args.file),
                                         filepath=args.file)
    sql.__description__ = 'Run an sql statement against ' \
        'the current ipydb connection.'

    @magic_arguments()
    @argument('-d', '--delimiter', action='store', default='/',
              help='Statement delimiter. Must be on a new line by itself')
    @argument('-i', '--interactive', action='store_true', default=False,
              help='Interactive mode - show and prompt each SQL statement')
    @argument('file', action='store', help='SQL script file')
    @line_magic
    def runsql(self, param=''):
        """Run delimited SQL statements from a file.

        SQL statements in the input file are expected to be delimited
        by '/' by itself on a new line. This can be overidden with the
        -d option.
        """
        args = parse_argstring(self.runsql, param)
        self.ipydb.run_sql_script(
            args.file,
            interactive=args.interactive,
            delimiter=args.delimiter)
    runsql.__description__ = 'Run delimited SQL ' \
        'statements from a file'

    @line_magic
    def tables(self, param=''):
        """Show a list of tables for the current db connection.

        Usage: %tables [GLOB1 GLOB2...]

        Show tables matching GLOB if given
        Example usage:
            %tables
                : lists all avaiable tables for the current connection
            %tables *p* *z*
                : shows tables having a 'p' or a 'z' in their name

        """
        self.ipydb.show_tables(*param.split())

    @line_magic
    def fields(self, param=''):
        """Show a list of fields and data types for the given table.

        Usage: %fields TABLE_GLOB[.FIELD_GLOB] [GLOB2...]

        Examples:
            fields person
                : shows fields for person table
            fields person.*id*
                : show fields for person table having `id` in their name
            fields *person*.*id*
                : show fields having id in their name for all tables
                  having 'person' in their name
        """
        self.ipydb.show_fields(*param.split())

    @line_magic
    def describe(self, param=''):
        """Print information about table: columns and keys."""
        self.ipydb.describe(*param.split())

    @line_magic
    def showsql(self, param=''):
        """Toggle SQL statement logging from SqlAlchemy."""
        if self.ipydb.show_sql:
            level = logging.WARNING
            self.ipydb.show_sql = False
        else:
            level = logging.INFO
            self.ipydb.show_sql = True
        logging.getLogger('sqlalchemy.engine').setLevel(level)
        print 'SQL logging %s' % ('on' if self.ipydb.show_sql else 'off')

    @line_magic
    def references(self, param=""):
        """Shows a list of all foreign keys that reference the given field.

        Usage: %references TABLE_NAME[.FIELD_NAME]

        If FIELD_NAME is ommitted, all fields in TABLE_NAME are checked as
        the target of a foreign key reference

        Examples:
            references person.id
                : shows all fields having a foreign key referencing person.id
        """
        if not param.strip() or len(param.split()) != 1:
            print "Usage: %references TABLE_NAME[.FIELD_NAME]"
            return
        self.ipydb.what_references(param)

    @line_magic
    def get_ipydb(self, param=''):
        """Return the active ipdyb plugin instance."""
        return self.ipydb

    @line_magic
    def joins(self, param=""):
        """Shows a list of all joins involving a given table.

        Usage: %joins TABLE_NAME
        """
        if not param.strip() or len(param.split()) != 1:
            print "Usage: %show_joins TABLE_NAME"
            return
        self.ipydb.show_joins(param)

    @line_magic
    def fks(self, param=""):
        """Shows a list of foreign keys for the given table.

        Usage: %fks TABLE_NAME
        """
        if not param.strip() or len(param.split()) != 1:
            print "Usage: %show_fks TABLE_NAME"
            return
        self.ipydb.show_fks(param)

    @line_magic
    def sqlformat(self, param=None):
        """Change the output format."""
        if not param or param not in ('csv', 'table'):
            print self.sqlformat.__doc__
        else:
            self.ipydb.sqlformat = param
            print "output format: %s" % self.ipydb.sqlformat

    @line_magic
    def connect(self, param):
        """Connect to a database using a configuration 'nickname'.

        Usage: %connect NICKNAME

        For this to work, you need to create a file called
        ~/.db-connections. This file is an "ini" file,
        parsable by python's ConfigParser.

        Here's an example of what ~/.db-connections might look like:

            [mydb]
            type: mysql
            username: root
            password: xxxx
            host: localhost
            database: employees

            [myotherdb]
            type: sqlite
            database: /path/to/file.sqlite

        Each database connection defined in ~/.db-connections is
        then referenceable via its section heading, or NICKNAME.

        Note: Before you can connect, you will need to install a python driver
        for your chosen database. For a list of recommended drivers,
        see the SQLAlchemy documentation:

            http://bit.ly/J3TBJh
        """
        self.ipydb.connect(param)

    @line_magic
    def connecturl(self, param):
        """Connect to a database using an SqlAlchemy style connection URL.

        Usage: %connecturl drivername://username:password@host/database
        Examples:
            %connecturl mysql://root@localhost/mydatabase
            %connecturl sqlite:///:memory:

        Note: Before you can connect, you will need to install a python driver
        for your chosen database. For a list of recommended drivers,
        see the SQLAlchemy documentation:

            http://bit.ly/J3TBJh
        """
        self.ipydb.connect_url(param)

    @line_magic
    def flushmetadata(self, arg):
        """Flush all ipydb's schema caches.

        Delete ipydb's in-memory cache of reflected schema information.
        Delete and re-create ipydb's sqlite information store.
        """
        self.ipydb.flush_metadata()

    @line_magic
    def rereflect(self, arg):
        """Force re-loading of completion metadata."""
        if not self.ipydb.connected:
            print self.ipydb.not_connected_message
            return
        self.ipydb.completion_accessor.get_metadata(
            self.ipydb.engine, force=True, noisy=True)

    @line_magic
    def saveconnection(self, arg):
        """Save current connection to ~/.db-connections file.

        Usage: %saveconnection NICKNAME

        After you have saved the connection, you can use the following to
        connect:
            %connect NICKNAME
        Note: if a configuration exists for NICKNAME it will be overwritten
        with the current engine's connection parameters.
        """
        if not self.ipydb.connected:
            print self.ipydb.not_connected_message
            return
        if not len(arg.strip()):
            print "Usage: %saveconnection NICKNAME. \n\n" + \
                "Please supply a NICKNAME to store the connection against."
            return
        self.ipydb.save_connection(arg)
