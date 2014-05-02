import unittest

from IPython.terminal.interactiveshell import TerminalInteractiveShell
import nose.tools as nt
import mock

from ipydb import magic, plugin


class ModelTest(unittest.TestCase):

    def setUp(self):
        self.ipydb = mock.MagicMock(spec=plugin.SqlPlugin)
        self.ipython = mock.MagicMock(spec=TerminalInteractiveShell)
        self.ipython.config = None
        self.magics = magic.SqlMagics(self.ipydb, self.ipython)

    def test_create_sql_alias(self):
        m = mock.MagicMock()
        alias = magic.create_sql_alias('select', m)
        alias(' -a -b -c * from table where thing = 0')
        m.sql.assert_called_with(
            '-a -b -c select * from table where thing = 0',
            None)

    @mock.patch('ipydb.ipydb_help')
    def test_ipydb_help(self, mockhelp):
        self.magics.ipydb_help()
        mockhelp.assert_called_once_with()

    def test_set_reflection(self):
        self.ipydb.do_reflection = False
        self.magics.set_reflection('')
        nt.assert_true(self.ipydb.do_reflection)
        self.magics.set_reflection('')
        nt.assert_false(self.ipydb.do_reflection)

    def test_engine(self):
        self.ipydb.get_engine.return_value = 'barry'
        eng = self.magics.engine('')
        nt.assert_equal('barry', eng)

    def test_debug(self):
        self.ipydb.debug = False
        self.magics.debug('')
        nt.assert_true(self.ipydb.debug)
        self.magics.debug('')
        nt.assert_false(self.ipydb.debug)

    def test_useless_tests_for_coverage_sake(self):
        self.magics.commit('')
        self.ipydb.commit.assert_called()
        self.magics.begin('')
        self.ipydb.begin.assert_called()
        self.magics.rollback('')
        self.ipydb.rollback.assert_called()
        self.magics.tables('')
        self.ipydb.show_tables.assert_called()
        self.magics.fields('')
        self.ipydb.show_fields.assert_called()
        self.ipydb.show_sql = True
        self.magics.showsql('')
        nt.assert_false(self.ipydb.show_sql)
        self.magics.references('a')
        self.ipydb.what_references.assert_called()
        self.magics.joins('a')
        self.ipydb.show_joins.assert_called()
        self.magics.fks('a')
        self.ipydb.show_fks.assert_called()
        self.magics.connect('a')
        self.ipydb.connect.assert_called()
        self.magics.connecturl('a')
        self.ipydb.connect_url.assert_called()

    def test_sql(self):
        thing = self.magics.sql('-r -f select * from blah where something = 1')
        nt.assert_is_not_none(thing)  # uhm, not sure what to check...
        lst = [{'x': 'y'}, {'e': 'f'}]
        d = {'a': 'b'}
        dct = {
            'zzz': d,
            'yyy': lst
        }

        # params and multiparams
        ret = self.ipydb.execute.return_value
        ret.returns_rows = True
        self.ipython.user_ns = dct
        self.magics.sql('-a zzz -m yyy select * from foo')
        self.ipydb.execute.assert_called_with(
            'select * from foo',
            params=d, multiparams=lst)

        ret.returns_rows = False
        ret.rowount = 2
        self.magics.sql('-a zzz -m yyy select * from foo')

        r = self.magics.sql('-r select * from foo')
        nt.assert_equal(ret, r)
