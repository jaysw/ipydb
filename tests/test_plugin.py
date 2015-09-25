from configparser import DuplicateSectionError
import re
from io import BytesIO, StringIO

from IPython.terminal.interactiveshell import TerminalInteractiveShell
import nose.tools as nt
import mock

from ipydb import plugin
from ipydb.metadata import model as m
from ipydb.metadata.model import Database


class TestSqlPlugin(object):

    def setup(self):
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
        self.mock_db_url = 'mysql://barry:xyz@zing.com/db'
        self.mengine.make_connection_url.return_value = self.mock_db_url
        self.sa_engine = mock.MagicMock()
        self.sa_engine.url.host = 'zing.com'
        self.sa_engine.url.database = 'db'
        self.mengine.from_url.return_value = self.sa_engine
        plugin.SqlPlugin.metadata_accessor = self.md_accessor
        self.ipython = mock.MagicMock(spec=TerminalInteractiveShell)
        self.ipython.config = None
        self.ipython.register_magics = mock.MagicMock()
        self.ipython.Completer = mock.MagicMock()
        self.ip = plugin.SqlPlugin(shell=self.ipython)

    def setup_run_sql(self, runsetup=False):
        if runsetup:
            self.setup()
        self.ip.engine.begin.return_value = self.ip.engine
        self.ip.trans_ctx = self.ip.engine
        self.ip.trans_ctx.conn = self.ip.engine
        s1 = u"update table foo set bar = 1 where baz = 2\n"
        s2 = u'delete from spam where eggs = 1\n'
        statements = u"{s1}/\n{s2}/\n".format(s1=s1, s2=s2)
        sio = StringIO(statements)
        mo = mock.mock_open(read_data=statements)
        handle = mo.return_value
        handle.readline = sio.readline
        self.ppatcher = mock.patch('ipydb.plugin.multi_choice_prompt')
        self.mock_open = mo
        self.s1 = s1
        self.s2 = s2

    def test_run_sql_script(self):
        self.setup_run_sql()
        with mock.patch('ipydb.plugin.open', self.mock_open, create=True):
            self.ip.run_sql_script('something', interactive=False)
            self.ip.engine.execute.assert_any_call(self.s1)
            self.ip.engine.execute.assert_any_call(self.s2)

    def run_sql_check(self, keypress):
        self.setup_run_sql()
        with mock.patch('ipydb.plugin.open', self.mock_open, create=True), \
                self.ppatcher as prompt:
            prompt.return_value = keypress
            self.ip.run_sql_script('something', interactive=True)
            if keypress in 'qn':
                nt.assert_equal(0, self.ip.engine.execute.call_count)
            elif keypress in 'ya':
                self.ip.engine.execute.assert_any_call(self.s1)
                self.ip.engine.execute.assert_any_call(self.s2)

    def test_run_sql_script_interactive(self):
        for keypress in 'ynqa':
                yield self.run_sql_check, keypress

    def test_rollback(self):
        self.ip.connected = False
        self.ip.rollback()
        nt.assert_is_none(self.ip.trans_ctx)
        self.ip.connected = True
        mockctx = mock.MagicMock()
        self.ip.trans_ctx = mockctx
        self.ip.rollback()
        mockctx.transaction.rollback.assert_called_once_with()

    def test_get_engine(self):
        self.ip.connected = False
        e = self.ip.get_engine()
        nt.assert_is_none(e)
        self.ip.connected = True
        e = self.ip.get_engine()
        nt.assert_equal(self.sa_engine, e)

    def test_transaction_prompt(self):
        self.ip.trans_ctx = mock.MagicMock()
        self.ip.trans_ctx.transaction.is_active = True
        nt.assert_equal(' *', self.ip.get_transaction_ps1())
        self.ip.connected = False
        nt.assert_equal('', self.ip.get_transaction_ps1())

    def test_prompt(self):
        self.ip.connected = False
        nt.assert_equal('', self.ip.get_db_ps1())
        self.ip.connected = True
        nt.assert_equal(' con1', self.ip.get_db_ps1())
        nt.assert_equal('', self.ip.get_transaction_ps1())
        self.md_accessor.reflecting.return_value = True
        nt.assert_equal(' !', self.ip.get_reflecting_ps1())
        self.ip.connected = False
        nt.assert_equal('', self.ip.get_reflecting_ps1())
        self.ip.connect_url(self.mock_db_url)
        nt.assert_equal(' zing/db', self.ip.get_db_ps1())

    def test_execute(self):
        self.mock_db.tables = ['foo']
        self.ip.execute('select foo')
        self.sa_engine.execute.assert_called_with('select * from foo')

    def test_execute_autotransaction(self):
        self.ip.flush_metadata()
        self.mock_db.tables = ['foo']
        stmt = 'insert into foo(id) values (1)'
        self.ip.execute(stmt)
        self.sa_engine.begin.assert_called_with()
        self.sa_engine.begin.return_value.conn.execute.assert_called_with(stmt)
        self.ip.commit()  # XXX: what to assert?

    def test_save_connection(self):
        self.ipython.ask_yes_no = mock.MagicMock(return_value=True)

        def sidey(name, engine, overwrite=False):
            if not overwrite:
                raise DuplicateSectionError('boom')
        self.mengine.save_connection = mock.MagicMock(
            side_effect=sidey)
        self.ip.save_connection('con1')
        self.mengine.save_connection.assert_called_with('con1', self.ip.engine,
                                                        overwrite=True)

    @mock.patch('ipydb.plugin.pager')
    def test_get_tables(self, pager):
        pagerio = BytesIO()
        pager.return_value.__enter__.return_value = pagerio
        self.ip.connected = False
        self.ip.show_tables()
        nt.assert_equal(0, pager.call_count)
        self.ip.connected = True
        self.mock_db.tables = 'foo bar'.split()
        self.ip.show_tables()
        output = pagerio.getvalue()
        nt.assert_in(b'foo', output)
        nt.assert_in(b'bar', output)

    @mock.patch('ipydb.plugin.pager')
    def test_get_tables_glob(self, pager):
        pagerio = BytesIO()
        pager.return_value.__enter__.return_value = pagerio
        self.mock_db.tables = 'foo bar'.split()
        self.ip.show_tables('f*')
        output = pagerio.getvalue()
        nt.assert_in(b'foo', output)
        nt.assert_not_in(b'bar', output)

    def setup_mock_describe_db(self, pager):
        self.pagerio = BytesIO()
        pager.return_value.__enter__.return_value = self.pagerio
        company = m.Table(id=1, name='company')
        cols = [
            m.Column(id=1, table_id=1, name='id', primary_key=True,
                     type="INTEGER", nullable=False, table=company),
            m.Column(id=2, table_id=1, name='name', type="INTEGER",
                     nullable=False, table=company),
        ]
        company.columns = cols
        company.indexes = [
            m.Index(name='someindex', id=1, table_id=1, table=company,
                    columns=[company.column('name')], unique=True)
        ]
        customer = m.Table(id=2, name='customer')
        columns = [
            m.Column(id=3, table_id=2, name='id', primary_key=True,
                     type="INTEGER", nullable=False, table=customer),
            m.Column(id=4, table_id=2, name='name', type="INTEGER",
                     nullable=False, table=customer),
            m.Column(id=5, table_id=2, name='company_id', type="INTEGER",
                     nullable=True, referenced_column_id=1,
                     constraint_name='company_id_fk',
                     referenced_column=company.column('id'),
                     table=customer)
        ]
        customer.columns = columns
        self.database = m.Database(tables=[company, customer])
        self.md_accessor.get_metadata.return_value = self.database

    @mock.patch('ipydb.plugin.pager')
    def test_describe_company(self, pager):
        self.setup_mock_describe_db(pager)
        self.ip.describe('company')
        output = self.pagerio.getvalue()
        nt.assert_regexp_matches(
            output.decode('utf8'), r'\*id\s+\|\s+INTEGER\s+\|\s+NOT NULL')
        nt.assert_regexp_matches(
            output.decode('utf8'), r'name\s+\|\s+INTEGER\s+\|\s+NOT NULL')
        pkre = re.compile(r'Primary Key \(\*\)\n\-+\s+id', re.M | re.I)
        nt.assert_regexp_matches(output.decode('utf8'), pkre)
        refs_re = re.compile(
            'References to company\n\-+\s+'
            'customer\(company_id\) references company\(id\)',
            re.M | re.I)
        nt.assert_regexp_matches(output.decode('utf8'), refs_re)

    @mock.patch('ipydb.plugin.pager')
    def test_describe_customer(self, pager):
        self.setup_mock_describe_db(pager)
        self.ip.describe('customer')
        output = self.pagerio.getvalue()
        nt.assert_regexp_matches(output.decode('utf8'), r'\*id.*INTEGER.*NOT NULL')
        nt.assert_regexp_matches(output.decode('utf8'), r'name.*INTEGER.*NOT NULL')
        nt.assert_regexp_matches(
            output.decode('utf8'), r'company_id\s+\|\s+INTEGER\s+\|\s+NULL')
        pkre = re.compile(r'Primary Key \(\*\)\n\-+\s+id', re.M | re.I)
        nt.assert_regexp_matches(output.decode('utf8'), pkre)
        fkre = re.compile(
            'Foreign Keys\n\-+\s+'
            'customer\(company_id\) references company\(id\)',
            re.M | re.I)
        nt.assert_regexp_matches(output.decode('utf8'), fkre)

    @mock.patch('ipydb.plugin.pager')
    def test_get_columns(self, pager):
        self.setup_mock_describe_db(pager)
        self.ip.show_fields()
        output = self.pagerio.getvalue()
        nt.assert_regexp_matches(
            output.decode('utf8'), 'company_id\s+INTEGER NULL')

    @mock.patch('ipydb.plugin.pager')
    def test_show_joins(self, pager):
        self.setup_mock_describe_db(pager)
        self.ip.show_joins('customer')
        output = self.pagerio.getvalue()
        expected = (b'customer inner join company on company.id = '
                    b'customer.company_id\n')
        nt.assert_equal(expected, output)

    @mock.patch('ipydb.plugin.pager')
    def test_what_references(self, pager):
        self.setup_mock_describe_db(pager)
        self.ip.what_references('company')
        output = self.pagerio.getvalue()
        expected = b'customer(company_id) references company(id)\n'
        nt.assert_equal(expected, output)

    @mock.patch('ipydb.plugin.pager')
    def test_show_fks(self, pager):
        self.setup_mock_describe_db(pager)
        self.ip.show_fks('customer')
        output = self.pagerio.getvalue()
        expected = b'customer(company_id) references company(id)\n'
        nt.assert_equal(expected, output)

    @mock.patch('ipydb.plugin.pager')
    def test_get_columns_glob(self, pager):
        self.setup_mock_describe_db(pager)
        self.ip.show_fields('*ustomer.na*')
        output = self.pagerio.getvalue()
        myre = re.compile(r'customer\n\-+\s+name\s+INTEGER NOT NULL')
        nt.assert_regexp_matches(output.decode('utf8'), myre)

    def teardown(self):
        self.pmeta.stop()
        self.pengine.stop()
