ipydb: Work with databases in IPython
=====================================
 
.. image:: https://travis-ci.org/jaysw/ipydb.svg?branch=master
     :target: https://travis-ci.org/jaysw/ipydb


.. image:: https://coveralls.io/repos/jaysw/ipydb/badge.png?branch=master
     :target: https://coveralls.io/r/jaysw/ipydb?branch=master


ipydb is an `IPython <http://ipython.org>`_ plugin for running SQL queries and viewing their results.

Usage
-----

.. code-block:: pycon

    $ ipython
    In [1] : %load_ext ipydb
    In [2] : %automagic on
    Automagic is ON, % prefix IS NOT needed for line magics.

    In [3] : connecturl mysql://user:pass@localhost/employees
    In [4] localhost/employees: tables
    departments
    dept_emp
    dept_manager
    employees
    salaries
    titles

    In [5] localhost/employees: fields departments
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
 - Cross-database support, thanks to SqlAlchemy: `supported databases <http://docs.sqlalchemy.org/en/rel_0_7/core/engines.html#supported-databases>`_ 


Installation
------------

To install ipydb:

.. code-block:: bash

    $ pip install git+https://github.com/jaysw/ipydb

You will need a python driver for your database of choice. For example:

.. code-block:: bash
 
    $ pip install mysql-python

ipydb uses `SqlAlchemy <http://www.sqlalchemy.org/>`_ to interact with databases.
See the `Supported Databases <http://docs.sqlalchemy.org/en/rel_0_7/core/engines.html#supported-databases>`_ page
for a (large!) list of supported `DB-API 2.0 <http://www.python.org/dev/peps/pep-0249/>`_ drivers and how to
write a connection URL for your particular database.


Start ipython and load the ipydb plugin:

.. code-block:: bash

    $ ipython
    In [1]: load_ext ipydb
    
    
Documentation
-------------
 
Documentation is available at: http://ipydb.readthedocs.org
 

Connecting to Databases
-----------------------
 
There are two ways to connect to a database with ipydb. Directly via a connection url, using
the ``connecturl`` magic function, or, using a connection 'nickname' with the ``connect`` magic function.

1. Using ``connecturl``
^^^^^^^^^^^^^^^^^^^^^^^

You can connect to a database using an SqlAlchemy style url as follows:

.. code-block:: pycon

    %connecturl drivername://username:password@host/database

Some examples:

.. code-block:: pycon

    In [3] : connecturl mysql://myuser:mypass@localhost/mydatabase
    In [4] : connecturl sqlite:///path/to/mydb.sqlite
    In [5] : connecturl sqlite:///:memory:

See the `SqlAlchemy Documentation <http://docs.sqlalchemy.org/en/rel_0_7/core/engines.html#database-urls>`_ for further information.

2. Using ``connect`` and a ``.db-connections`` configuration file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For this to work, you need to create a file called
``.db-connections`` located in your home directory.
``.db-connections`` is an "ini" formatted file,
parsable by python's ConfigParser module.

Here's an example of what ``~/.db-connections`` might look like:

.. code-block:: ini

    [mydb]
    type: mysql
    username: root
    password: xxxx
    host: localhost
    database: employees

    [myotherdb]
    type: sqlite
    database: /path/to/file.sqlite

Each database connection defined in ``~/.db-connections`` is
then referenceable via its [section heading]. So with the
above ``.db-connections`` file, the following examples would work:

.. code-block:: pycon

    In [6] : connect mydb
    In [7] mydb : connect myotherdb

