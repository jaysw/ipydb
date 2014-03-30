import itertools
import unittest

import nose.tools as nt

from ipydb.metadata import model as m


class ModelTest(unittest.TestCase):

    def setUp(self):
        foo = m.Table(id=1, name='foo')
        foo.columns = [
            m.Column(id=1, table_id=1, name='first', type='VARCHAR(10)',
                     primary_key=True, nullable=False, table=foo),
            m.Column(id=2, table_id=1, name='second', type='INT',
                     primary_key=True, nullable=False, table=foo),
            m.Column(id=3, table_id=1, name='third', type='DATE',
                     primary_key=True, nullable=False, table=foo,
                     default_value='bananas'),
        ]
        bar = m.Table(id=2, name='bar')
        bar.columns = [
            m.Column(id=4, table_id=2, name='thing', type='atype',
                     primary_key=True, nullable=True, table=bar),
        ]
        baz = m.Table(id=3, name='baz')
        baz.columns = [
            m.Column(id=5, table_id=3, name='other', type='atype',
                     primary_key=True, nullable=True, table=baz),
        ]
        lur = m.Table(id=4, name='lur')
        lur.columns = [
            m.Column(id=6, table_id=4, name='foo_id', type='atype',
                     primary_key=True, nullable=False, table=lur,
                     referenced_column_id=1, constraint_name='foo_fk'),
            m.Column(id=7, table_id=4, name='bar_id', type='atype',
                     primary_key=True, nullable=False, table=lur,
                     referenced_column_id=4, constraint_name='bar_fk'),
        ]
        self.foo = foo
        self.bar = bar
        self.baz = baz
        self.lur = lur
        self.tables = [foo, bar, baz, lur]
        self.db = m.Database(self.tables)

        # setup join asociations.
        lur.columns[0].referenced_column = foo.columns[0]
        lur.columns[1].referenced_column = bar.columns[0]
        foo.columns[0].referenced_by = [lur.columns[0]]
        bar.columns[0].referenced_by = [lur.columns[1]]
        self.lur_foo = {m.ForeignKey('lur', ('foo_id',), 'foo', ('first',))}
        self.lur_bar = {m.ForeignKey('lur', ('bar_id',), 'bar', ('thing',))}

        # an index
        self.idx = m.Index(id=1, name='myidx', unique=False, table_id=4,
                           table=lur, columns=[lur.columns[0]])
        lur.indexes = [self.idx]
        lur.columns[0].indexes = [self.idx]

    def test_init(self):
        nt.assert_false(self.db.isempty)
        nt.assert_false(self.db.reflecting)
        expected = ['foo', 'bar', 'baz', 'lur']
        nt.assert_equal(sorted(expected), sorted(self.db.tablenames()))
        nt.assert_equal(self.foo.columns[0], self.foo.column('first'))

    def test_sql_default(self):
        expectations = {
            ('sometype', True, None): 'NULL',
            ('sometype', False, None): '',
            ('sometype', False, 'somedefault'): "'somedefault'",
            ('INT', False, None): '0',
            ('VARCHAR', False, None): "'hello'",
            ('DATE', False, None): "current_date",
            ('TIME', False, None): "current_time",
            ('TIMESTAMP', False, None): "current_timestamp",
        }
        for (typ, nullable, default), expected in expectations.iteritems():
            col = m.Column(id=1, table_id=1, name='first',
                           primary_key=True,
                           type=typ,
                           nullable=nullable,
                           default_value=default)
            nt.assert_equal(expected, m.sql_default(col))

    def test_columns(self):
        cols = itertools.chain(*[t.columns for t in self.tables])
        nt.assert_equal(sorted(cols), sorted(self.db.columns))

    def test_fieldnames(self):
        cols = itertools.chain(*[t.columns for t in self.tables])
        fieldnames = [c.name for c in cols]
        nt.assert_equal(sorted(fieldnames), sorted(self.db.fieldnames()))

        lur_fields = ['bar_id', 'foo_id']
        nt.assert_equal(lur_fields, sorted(self.db.fieldnames('lur')))

        lur_dfields = ['lur.bar_id', 'lur.foo_id']
        nt.assert_equal(lur_dfields,
                        sorted(self.db.fieldnames('lur', dotted=True)))

        nt.assert_equal(set(), self.db.fieldnames('asfd'))

    def test_get_joins(self):

        nt.assert_equal(self.lur_foo, self.db.get_joins('lur', 'foo'))
        nt.assert_equal(self.lur_bar, self.db.get_joins('lur', 'bar'))
        nt.assert_equal(set(), self.db.get_joins('foo', 'bar'))
        nt.assert_equal(set(), self.db.get_joins('xxx', 'bar'))

    def test_tables_referencing(self):
        nt.assert_equal({'lur'}, self.db.tables_referencing('foo'))
        nt.assert_equal({'lur'}, self.db.tables_referencing('bar'))
        nt.assert_equal(set(), self.db.tables_referencing('xx'))

    def test_fields_referencing(self):
        nt.assert_equal(self.lur_foo, set(self.db.fields_referencing('foo')))
        nt.assert_equal(self.lur_bar, set(self.db.fields_referencing('bar')))
        nt.assert_equal(set(), set(self.db.fields_referencing('baz')))

    def test_foreign_keys(self):
        exp = set()
        exp.update(self.lur_foo, self.lur_bar)
        nt.assert_equal(exp, set(self.db.foreign_keys('lur')))
        nt.assert_equal(set(), set(self.db.foreign_keys('foo')))
        nt.assert_equal(set(), set(self.db.foreign_keys('not_a_table')))

    def test_all_joins(self):
        exp = set()
        exp.update(self.lur_foo, self.lur_bar)
        nt.assert_equal(exp, set(self.db.all_joins('lur')))
        nt.assert_equal(self.lur_foo, set(self.db.all_joins('foo')))

    def test_insert_statement(self):
        exp = ("insert into foo (first, second, third) "
               "values ('hello', 0, 'bananas')")
        nt.assert_equal(exp, self.db.insert_statement('foo'))

    def test_indexes(self):
        nt.assert_equal({self.idx}, set(self.db.indexes('lur')))
        nt.assert_equal(set(), set(self.db.indexes('foo')))

    def test_as_join(self):
        fk = iter(self.lur_foo).next()
        exp = 'foo inner join lur on foo.first = lur.foo_id'
        nt.assert_equal(exp, fk.as_join())
