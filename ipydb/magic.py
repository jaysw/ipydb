# -*- coding: utf-8 -*-

"""
IPython magic commands registered by ipydb

:copyright: (c) 2012 by Jay Sweeney.
:license: see LICENSE for more details.
"""

from IPython.core.magic import Magics, magics_class, line_magic, line_cell_magic

@magics_class
class SqlMagics(Magics):

    def __init__(self, ipydb, *a, **kw):
        super(SqlMagics, self).__init__(*a, **kw)
        self.ipydb = ipydb

    @line_magic
    def ipydb_help(self, *args):
        """Show this help message"""
        from ipydb import ipydb_help # XXX: recursive import problem...
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
        result = self.ipydb.execute(param)
        if result and result.returns_rows:
            self.ipydb.render_result(result)

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
        result = self.ipydb.execute('select ' + param)
        if result:
            self.ipydb.render_result(result)

    @line_magic
    def show_tables(self, param=''):
        """Show a list of tables for the current db connection 

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
        """Show a list of fields and data types for the given table

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
    def sqlformat(self, param=None):
        """Usage: %sqlformat <table|csv>"""
        if not param or param not in ('csv', 'table'):
            print self.sqlformat.__doc__
        else:
            self.ipydb.sqlformat = param
            print "output format: %s" % self.ipydb.sqlformat

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
        self.ipydb.connect(param)

    @line_magic
    def connect_url(self, param):
        """Connect to a database using an SqlAlchemy style connection URL

        Usage: %connect_url drivername://username:password@host/database
        Examples: 
            %connect_url mysql://root@localhost/mydatabase
            %connect_url sqlite:///:memory:
        You will need to ensure that you have installed a python driver for your chosen database.
        see: http://docs.sqlalchemy.org/en/rel_0_7/core/engines.html#supported-databases"""
        self.ipydb.connect_url(param)
