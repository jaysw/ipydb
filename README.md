ipydb: Work with databases in IPython
=========================

ipydb is an [IPython](http://ipython.org/) plugin for running SQL queries and viewing their results.

Usage
-----

    $ ipython
    In [1] : %load_ext ipydb
    In [2] : %automagic on
    Automagic is ON, % prefix IS NOT needed for line magics.

    In [3] : connect_url mysql://user:pass@localhost/employees
    In [4] localhost/employees: show_tables
    departments
    dept_emp
    dept_manager
    employees
    salaries
    titles

    In [5] localhost/employees: show_fields departments
    departments
    -----------
        dept_name                          VARCHAR(40)
        dept_no                            CHAR(4)

    In [6] localhost/employees: select * from departments order by dept_name
    +---------+--------------------+
    | dept_no | dept_name          |
    +---------+--------------------+
    | d009    | Customer Service   |
    | d005    | Development        |
    | d002    | Finance            |
    | d003    | Human Resources    |
    | d001    | Marketing          |
    | d004    | Production         |
    | d006    | Quality Management |
    | d008    | Research           |
    | d007    | Sales              |


Features
--------

 - Tab-completion of table names and fields
 - View query results in ascii-table format piped through less
 - Single-line or multi-line query editing
 - Tab-completion metadata is read in the background and persisted across sessions
 - Cross-database support, thanks to SqlAlchemy: [supported databases](http://docs.sqlalchemy.org/en/rel_0_7/core/engines.html#supported-databases)


Installation
------------

To install ipydb:

    $ pip install git+https://github.com/jaysw/ipydb

You will need a python driver for your database of choice. For example:

    $ pip install mysql-python

ipydb uses [SqlAlchemy](http://www.sqlalchemy.org/) to interact with databases. See the [Supported Databases](http://docs.sqlalchemy.org/en/rel_0_7/core/engines.html#supported-databases) page for a (large!) list of supported [DB-API 2.0](http://www.python.org/dev/peps/pep-0249/) drivers and how to write a connection URL for your particular database.

    