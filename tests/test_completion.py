import collections
import itertools
import unittest

import mock
from mock import patch
import nose.tools as nt

from ipydb import completion
from ipydb.metadata import model as m


class Event(object):

    def __init__(self, command='', line='', symbol='', text_until_cursor=''):
        self.command = command
        self.line = line
        self.symbol = symbol
        self.text_until_cursor = text_until_cursor


class CompleterTest(unittest.TestCase):

    def setUp(self):
        self.db = mock.Mock(spec=m.Database)
        self.completer = completion.IpydbCompleter(get_db=lambda: self.db)
        self.data = {
            'foo': ['first', 'second', 'third'],
            'bar': ['thing'],
            'baz': ['other'],
            'lur': ['foo_id', 'bar_id']
        }

        self.db.tablenames.return_value = self.data.keys()
        self.db.fieldnames = mock.MagicMock(side_effect=self.mock_fieldnames)
        # setup some joins
        lur_foo = m.ForeignKey(table='lur', columns=('foo_id',),
                               reftable='foo', refcolumns=('first',))
        lur_bar = m.ForeignKey(table='lur', columns=('bar_id',),
                               reftable='bar', refcolumns=('thing',))
        joins = collections.defaultdict(set)
        joins.update({
            'foo': {lur_foo},
            'bar': {lur_bar},
            'baz': set(),
            'lur': {lur_foo, lur_bar},
        })
        self.db.all_joins = mock.MagicMock(
            side_effect=lambda t: joins.get(t, set()))

        def mock_get_joins(t1, t2):
            return joins.get(t1, set()) & joins.get(t2, set())

        self.db.get_joins = mock.MagicMock(side_effect=mock_get_joins)

    def mock_fieldnames(self, table=None, dotted=False):
        """Pretends to be Database.fieldnames() using self.data"""
        if table is None:
            if not dotted:
                return itertools.chain(*self.data.values())
            else:
                return ['%s.%s' % (t, c) for t, cols in self.data.iteritems()
                        for c in cols]
        if dotted:
            return ['%s.%s' % (table, col) for col in self.data[table]]
        else:
            return self.data[table]

    def test_table_name(self):
        result = self.completer.table_name(Event(symbol='ba'))
        nt.assert_equal(sorted(result), ['bar', 'baz'])

    def test_dotted_expressions(self):
        result = self.completer.dotted_expression(Event(symbol='foo.'))
        nt.assert_equal(result, ['foo.first', 'foo.second', 'foo.third'])

        result = self.completer.dotted_expression(Event(symbol='foo.se'))
        nt.assert_equal(result, ['foo.second'])

        result = self.completer.dotted_expression(Event(symbol='bar.'))
        nt.assert_equal(result, ['bar.thing'])

        # where the table is unknown (e.g. some alias that we haven't parsed)
        # return a match on ANY field. I think this is better than returning
        # no matches for now.
        result = self.completer.dotted_expression(
            Event(symbol='something.thin'))
        nt.assert_equal(result, ['thing'])

        # this one is a bit crazy. show every possible fieldname...
        result = self.completer.dotted_expression(
            Event(symbol='something.'))
        nt.assert_equal(result,
                        sorted('something.' + c
                               for c in itertools.chain(*self.data.values())))

    def test_expand_table_dot_star(self):
        result = self.completer.dotted_expression(Event(symbol='foo.*'))
        nt.assert_equal(result, ['foo.first, foo.second, foo.third'])

    def test_expand_simple_select(self):
        result = self.completer.expand_two_token_sql(
            Event(line="select foo", symbol="foo"))
        nt.assert_equal(result, ['foo.* from foo'])

    def test_expand_insert_statement(self):
        insert_statement = 'insert into foo blah sure thing'
        self.db.insert_statement.return_value = insert_statement
        result = self.completer.expand_two_token_sql(
            Event(line="insert foo", symbol="foo"))
        nt.assert_equal(result, [' into foo blah sure thing'])

    def test_is_valid_join_expression(self):
        valid_joins = [
            'lur**foo',
            'foo**lur',
            'lur**bar',
            'bar**lur',
            'bar**lur**foo',
            'bar**lur**foo**foo**bar',
        ]
        invalid_joins = [
            'bar**baz',
            'foo**baz',
            'bar**z',
            'bar**',
        ]
        for valid in valid_joins:
            nt.assert_true(self.completer.is_valid_join_expression(valid))
        for invalid in invalid_joins:
            nt.assert_false(self.completer.is_valid_join_expression(invalid))

    def test_expand_join_expression(self):
        expansions = {
            'not**real': 'not**real',
            'lur**foo': 'lur inner join foo on lur.foo_id = foo.first ',
        }
        for k, v in expansions.iteritems():
            nt.assert_equal(self.completer.expand_join_expression(k), v)

    def test_join_shortcut(self):
        expectations = {
            'lur**': ['lur**bar', 'lur**foo'],
            'foo**': ['foo**lur'],
            'baz**': [],
            'lur**foo**': ['lur**foo**bar', 'lur**foo**foo', 'lur**foo**lur'],
            'lur**ba': ['lur**bar'],
            'lur**bar**f': ['lur**bar**foo'],
            'lur**bar': ['lur inner join bar on lur.bar_id = bar.thing '],

        }
        for symbol, expected in expectations.iteritems():
            actual = self.completer.join_shortcut(Event(symbol=symbol))
            nt.assert_equal(expected, actual)

    def test_sql_format(self):
        expectations = {
            '': ['csv', 'table'],
            'cs': ['csv'],
            'ta': ['table']
        }
        for symbol, expected in expectations.iteritems():
            actual = self.completer.sql_format(Event(symbol=symbol))
            nt.assert_equal(expected, actual)

    def mock_config(self, mock_getconfigs):
        """mocks out the getconfigs() call in ipydb.completion"""
        confignames = 'employees northwind something'.split()
        # getconfigs()[1].keys()
        mock_getconfigs.return_value.__getitem__.return_value\
            .keys.return_value = confignames

    @patch('ipydb.completion.getconfigs')
    def test_connection_nickname(self, mock_getconfigs):
        self.mock_config(mock_getconfigs)
        expectations = {
            '': ['employees', 'northwind', 'something'],
            'emp': ['employees'],
            'no': ['northwind']
        }
        for symbol, expected in expectations.iteritems():
            actual = self.completer.connection_nickname(Event(symbol=symbol))
            nt.assert_equal(expected, actual)

    def test_sql_statement(self):
        expectations = {
            ('select foo', 'foo'): ['foo.* from foo'],
            ('select foo.fir', 'foo.fir'): ['foo.first'],
            ('select foo**lu', 'foo**lu'): ['foo**lur'],
            ('select foo.first foo.se', 'foo.se'): ['foo.second'],
        }
        for (line, symbol), expected in expectations.iteritems():
            actual = self.completer.sql_statement(
                Event(line=line, symbol=symbol))
            nt.assert_equal(expected, actual)

    @patch('ipydb.completion.getconfigs')
    def test_complete(self, mock_getconfigs):
        self.mock_config(mock_getconfigs)
        expectations = {
            ('connect nor', 'connect', 'nor'): ['northwind'],
            ('sqlformat ta', 'sqlformat', 'ta'): ['table'],
            ('references ba', 'references', 'ba'): ['bar', 'baz'],
            ('tables ba', 'tables', 'ba'): ['bar', 'baz'],
            ('fields fo', 'fields', 'fo'): ['foo'],  # needs more!
            ('joins fo', 'joins', 'fo'): ['foo'],
            ('fks fo', 'fks', 'fo'): ['foo'],
            ('describe fo', 'describe', 'fo'): ['foo'],
            ('sql sele', 'sql', 'sele'): ['select'],
            ('%sql sele', '%sql', 'sele'): ['select'],
            ('sql select foo.fi', 'sql', 'foo.fi'): ['foo.first'],
            ('select foo.fi', 'sql', 'foo.fi'): ['foo.first'],
            ('runsql anything', 'runsql', 'anything'): None,
            ('foo = %select -r foo.fi', 'select', 'foo.fi'): ['foo.first'],
            ('zzzz', 'zzzz', 'zzzz'): None,
        }
        for (line, command, symbol), expected in expectations.iteritems():
            actual = self.completer.complete(
                Event(line=line, symbol=symbol, command=command))
            nt.assert_equal(expected, actual)

    def mock_ipy_magic(self, s):
        """mock for completion.get_ipydb and completion.ipydb_complete()"""
        if s != 'get_ipydb':
            raise Exception('something bad happened')
        m = mock.MagicMock()
        sqlplugin = m.return_value
        sqlplugin.debug = True
        sqlplugin.completer = self.completer
        return sqlplugin

    def test_ipydb_complete(self):
        mock_ipy = mock.MagicMock()
        mock_ipy.magic = mock.MagicMock(side_effect=self.mock_ipy_magic)
        result = completion.ipydb_complete(
            mock_ipy,
            Event(line='select fo', command='select', symbol='fo',
                  text_until_cursor='select fo'))
        nt.assert_true('foo', result)

    def test_monkey_string(self):
        ms = completion.MonkeyString('hello w', 'something hello w')
        nt.assert_true(ms.startswith('hello w'))
        nt.assert_equal(ms, 'something hello w')
        nt.assert_false(ms.startswith('other unrelated thing'))

    def test_exceptions_are_surpressed(self):
        mock_ipy = mock.MagicMock()
        mock_ipydb = self.mock_ipy_magic('get_ipydb')
        mock_ipydb.debug = False
        mock_ipy.magic.return_value = mock_ipydb

        def kaboom(*args, **kw):
            raise Exception('ka ka ka boo boo booom!')

        mock_ipydb.completer = mock.MagicMock()
        mock_ipydb.completer.complete = mock.MagicMock(side_effect=kaboom)
        completion.ipydb_complete(
            mock_ipy,
            Event(line='select fo', command='select', symbol='fo',
                  text_until_cursor='select fo'))
