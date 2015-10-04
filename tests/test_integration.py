"""Some integration tests using the chinook example db."""
from __future__ import print_function

from io import BytesIO, StringIO
import shutil
import sys

from IPython.terminal.interactiveshell import TerminalInteractiveShell
import mock
import nose.tools as nt

import ipydb
from ipydb import plugin, engine


EXAMPLEDB = 'sqlite:///tests/dbs/temp.sqlite'


class TestIntegration(object):

    def setup(self):
        shutil.copyfile('tests/dbs/chinook.sqlite', 'tests/dbs/temp.sqlite')
        self.pgetconfigs = mock.patch('ipydb.plugin.engine.getconfigs')
        mgetconfigs = self.pgetconfigs.start()
        mgetconfigs.return_value = None, []
        self.ipython = mock.MagicMock(spec=TerminalInteractiveShell)
        self.pget_metadata_engine = mock.patch(
            'ipydb.metadata.get_metadata_engine')
        mget_engine = self.pget_metadata_engine.start()
        self.md_engine = engine.from_url('sqlite:///:memory:')
        mget_engine.return_value = ('memory', self.md_engine)
        self.ipython.config = None
        self.ipython.register_magics = mock.MagicMock()
        self.ipython.Completer = mock.MagicMock()
        self.ipydb = plugin.SqlPlugin(shell=self.ipython)
        self.ipydb.metadata_accessor.debug = True  # turn off threading
        self.m = self.ipydb.auto_magics
        self.out = BytesIO()
        self.ppager = mock.patch('ipydb.plugin.pager', spec=plugin.pager)
        self.mockpager = self.ppager.start()
        self.mockpager.return_value.__enter__.return_value = self.out

    def test_it(self):
        self.m.connecturl(EXAMPLEDB)
        self.ipydb.get_reflecting_ps1()
        self.m.flushmetadata('')
        self.m.describe('Album')
        print(self.out.getvalue())

    def test_debug(self):
        self.m.debug_ipydb('')
        nt.assert_true(self.ipydb.debug)
        nt.assert_true(self.ipydb.metadata_accessor.debug)

    def test_help(self):
        ipydb.ipydb_help()  # XXX: assert somthing...

    def test_other(self):
        self.m.connecturl(EXAMPLEDB)
        self.m.sql('-p select * from Album', 'where albumId = 1')
        self.m.sql('-f select * from Album')
        self.m.runsql('tests/dbs/test.sql')
        self.m.sqlformat('vsc')
        self.m.sqlformat('csv')
        self.m.rereflect('')
        print(self.out.getvalue())

    def test_insert(self):
        self.m.connecturl(EXAMPLEDB)
        output = ''
        try:
            sys.stdout = BytesIO()
            self.m.sql("insert into Genre (Name) values ('Cronk')")
            output = sys.stdout.getvalue()
        finally:
            sys.stdout = sys.__stdout__
        nt.assert_in(b'1 row affected', output)

    def teardown(self):
        self.pgetconfigs.stop()
        self.pget_metadata_engine.stop()
        self.ppager.stop()
