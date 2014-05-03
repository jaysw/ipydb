import unittest

from IPython.terminal.interactiveshell import TerminalInteractiveShell
import nose.tools as nt
import mock

from ipydb import plugin
from ipydb.metadata.model import Database


class SqlPluginTest(unittest.TestCase):

    def setUp(self):
        self.pmeta = mock.patch('ipydb.metadata.MetaDataAccessor')
        self.md_accessor = self.pmeta.start()
        self.mock_db = mock.MagicMock(spec=Database)
        self.md_accessor.get_metadata.return_value = self.mock_db
        self.pengine = mock.patch('ipydb.plugin.engine')
        self.mengine = self.pengine.start()
        configs = {
            'con1': {
                'type': 'mysql',
                'username': 'barry',
                'password': 'xyz',
                'host': 'zing',
                'database': 'db'
            }
        }
        self.mengine.getconfigs.return_value = ('con1', configs)
        self.mengine.make_connection_url.return_value =  \
            'mysql://barry:xyz@zing/db'
        self.sa_engine = mock.MagicMock()
        self.mengine.from_url.return_value = self.sa_engine
        plugin.SqlPlugin.metadata_accessor = self.md_accessor
        self.ipython = mock.MagicMock(spec=TerminalInteractiveShell)
        self.ipython.config = None
        self.ipython.register_magics = mock.MagicMock()
        self.ipython.Completer = mock.MagicMock()
        self.ip = plugin.SqlPlugin(shell=self.ipython)

    def test_prompt(self):
        nt.assert_equal(' con1', self.ip.get_db_ps1())
        nt.assert_equal('', self.ip.get_transaction_ps1())
        self.md_accessor.reflecting.return_value = True
        nt.assert_equal(' !', self.ip.get_reflecting_ps1())

    def test_execute(self):
        self.mock_db.tables = ['foo']
        self.ip.execute('select foo')
        self.sa_engine.execute.assert_called_with('select * from foo')

    def test_execute_autotransaction(self):
        self.ip.flush_metadata()
        self.mock_db.tables = ['foo']
        stmt = 'insert into foo(id) values (1)'
        self.ip.execute(stmt)
        self.sa_engine.begin.assert_called()
        self.sa_engine.begin.return_value.conn.execute.assert_called_with(stmt)
        self.ip.commit()  # XXX: what to assert?

    def tearDown(self):
        self.pmeta.stop()
        self.pengine.stop()
