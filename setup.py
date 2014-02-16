#!/usr/bin/env python
from setuptools import setup
import ipydb

requires = ['SQLAlchemy', 'ipython>=1.0', 'python-dateutil', 'sqlparse']
tests_require = ['nose']
description = "An IPython extension to help you write and run SQL statements"

setup(
    name='ipydb',
    version=ipydb.__version__,
    description=description,
    author='Jay Sweeney',
    author_email='writetojay@gmail.com',
    url='http://github.com/jaysw/ipydb',
    packages=['ipydb'],
    package_dir={'ipydb': 'ipydb'},
    include_package_data=True,
    zip_safe=False,
    install_requires=requires,
    test_suite='nose.collector',
    tests_require=tests_require
)
