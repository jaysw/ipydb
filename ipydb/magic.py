# -*- coding: utf-8 -*-

"""
IPython magic commands registered by ipydb

:copyright: (c) 2012 by Jay Sweeney.
:license: see LICENSE for more details.
"""

from IPython.core.magic import Magics, magics_class, \
    line_magic, line_cell_magic
from IPython.core.magic_arguments import magic_arguments, \
    argument, parse_argstring


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
        """Return sqlalchemy engine reference to the current ipydb connection.
        """
        return self.ipydb.get_engine()

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
    @argument('sql_statement',  help='The SQL statement to run', nargs="*")
    @line_cell_magic
    def sql(self, args='', cell=None):
        """Run an sql statement against the current ipydb connection.

        Examples:

            %sql select first_name from person where first_name like 'J%'

        Also works as a multi-line ipython command.

            %%sql
                select
                    id, name, description
                from
                    my_table
                where
                    id < 10

        To get a result set back instead of printing the query results:

            results = %sql -r select first_name from employees
            for row in results:
                do_things_with(row.first_name)

        """
        args = parse_argstring(self.sql, args)
        sql = ' '.join(args.sql_statement)
        if cell is not None:
            sql += '\n' + cell
        result = self.ipydb.execute(sql)
        if args.ret:
            return result
        if result and result.returns_rows:
            self.ipydb.render_result(result)

    @line_cell_magic
    def select(self, param='', cell=None):
        """Run a select statement against the current connection.

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
        result = self.ipydb.execute('select ' + param)
        if result:
            self.ipydb.render_result(result)

    @line_magic
    def show_tables(self, param=''):
        """Show a list of tables for the current db connection.

        Usage: %show_tables [GLOB1 GLOB2...]

        Show tables matching GLOB if given
        Example usage:
            %show_tables
                : lists all avaiable tables for the current connection
            %show_tables *p* *z*
                : shows tables having a 'p' or a 'z' in their name

        """
        self.ipydb.show_tables(*param.split())

    @line_magic
    def show_fields(self, param=''):
        """Show a list of fields and data types for the given table.

        Usage: %show_fields TABLE_GLOB[.FIELD_GLOB] [GLOB2...]

        Examples:
            show_fields person
                : shows fields for person table
            show_fields person.*id*
                : show fields for person table having `id` in their name
            show_fields *person*.*id*
                : show fields having id in their name for all tables
                  having 'person' in their name
        """
        self.ipydb.show_fields(*param.split())

    @line_magic
    def what_references(self, param=""):
        """Shows a list of all foreign keys that reference the given field.

        Usage: %what_references TABLE_NAME[.FIELD_NAME]

        If FIELD_NAME is ommitted, all fields in TABLE_NAME are checked as
        the target of a foreign key reference

        Examples:
            what_referenes person.id
                : shows all fields having a foreign key referencing person.id
        """
        if not param.strip() or len(param.split()) != 1:
            print "Usage: %what_references TABLE_NAME[.FIELD_NAME]"
            return
        self.ipydb.what_references(param)

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
    def connect_url(self, param):
        """Connect to a database using an SqlAlchemy style connection URL.

        Usage: %connect_url drivername://username:password@host/database
        Examples:
            %connect_url mysql://root@localhost/mydatabase
            %connect_url sqlite:///:memory:

        Note: Before you can connect, you will need to install a python driver
        for your chosen database. For a list of recommended drivers,
        see the SQLAlchemy documentation:

            http://bit.ly/J3TBJh
        """
        self.ipydb.connect_url(param)

    @line_magic
    def flush_metadata(self, arg):
        """Flush all ipydb's schema caches.

        Delete ipydb's in-memory cache of reflected schema information.
        Delete and re-create ipydb's sqlite information store.
        """
        self.ipydb.flush_metadata()
