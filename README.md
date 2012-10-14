ipydb
=====

An extension for IPython to help with database work


Usage:
======
    $ ipython
    In [1]: %load_ext ipydb           
    In [2]: %connect_url mysql://user:pass@localhost/mydbname
    In [3]: select * from person order by id desc