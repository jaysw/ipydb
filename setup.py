#!/usr/bin/env python
from codecs import open

from setuptools import setup
import ipydb

requires = ['SQLAlchemy', 'ipython>=1.0', 'python-dateutil', 'sqlparse',
            'future']
tests_require = ['nose', 'mock==1.0.1']
extras_require = {'doc': ['Sphinx==1.2.3', 'sphinx-rtd-theme==0.1.6'],
                  'notebook': ['pandas>=0.16.2']
                  }
description = "An IPython extension to help you write and run SQL statements"

with open('README.rst', 'r', 'utf-8') as f:
    readme = f.read()

setup(
    name='ipydb',
    version=ipydb.__version__,
    description=description,
    long_description=readme,
    author='Jay Sweeney',
    author_email='writetojay@gmail.com',
    url='http://github.com/jaysw/ipydb',
    packages=['ipydb', 'ipydb.metadata'],
    package_dir={'ipydb': 'ipydb'},
    package_data={'': ['LICENSE']},
    include_package_data=True,
    zip_safe=False,
    license='Apache 2.0',
    install_requires=requires,
    extras_require=extras_require,
    test_suite='nose.collector',
    tests_require=tests_require,
    classifiers=(
        "Development Status :: 4 - Beta",
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Environment :: Console',
        'Framework :: IPython',
        'Topic :: Database',
    )
)
