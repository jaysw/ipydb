import collections
import itertools
import unittest

import mock
import nose.tools as nt

from ipydb.completion import IpydbCompleter
from ipydb.metadata import model as m


class Event(object):

    def __init__(self, command='', line='', symbol=''):
        self.command = command
        self.line = line
        self.symbol = symbol


class TestCompletion(unittest.TestCase):

    def setUp(self):
        self.db = mock.Mock(spec=m.Database)
        self.completer = IpydbCompleter(get_db=lambda: self.db)
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
