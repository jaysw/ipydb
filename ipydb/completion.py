# -*- coding: utf-8 -*-

"""
This module provides functionality for readline-style
tab-completion of SQL statements and other ipydb commands.
"""
from __future__ import print_function
import itertools
import logging
import re

from sqlalchemy.sql.compiler import RESERVED_WORDS

from ipydb.engine import getconfigs
from ipydb.magic import SQL_ALIASES

log = logging.getLogger(__name__)
reassignment = re.compile(r'^\w+\s*=\s*%((\w+).*)')


def get_ipydb(ipython):
    """Return the active ipydb instance."""
    return ipython.magic('get_ipydb')


def ipydb_complete(self, event):
    """Returns a list of suggested completions for event.symbol.

    Note: This function is bound to an ipython shell instance
          and called on tab-presses by ipython.
    Args:
        event: see IPython.core.completer
    Returns:
        A list of candidate strings which complete the input text
        or None to propagate completion to other handlers or
        return [] to suppress further completion
    """
    sqlplugin = get_ipydb(self)
    try:
        if sqlplugin:
            if sqlplugin.debug:
                print('complete: sym=[%s] line=[%s] tuc=[%s]' % (
                    event.symbol, event.line, event.text_until_cursor))
            completions = sqlplugin.completer.complete(event)
            if sqlplugin.debug:
                print('completions:', completions)
            return completions
    except Exception as e:
        print(repr(e))
        if sqlplugin and sqlplugin.debug:
            import traceback
            traceback.print_exc()
    return None


def match_lists(lists, text, appendfunc=None, sort=True):
    """Helper to substring-match text in a list-of-lists."""
    n = len(text)
    results = None
    if appendfunc is None:
        results = []
    for word in itertools.chain(*lists):
        if word[:n] == text:
            if appendfunc:
                appendfunc(word)
            else:
                results.append(word)
    if appendfunc is None:
        return results


class MonkeyString(str):
    """This is to avoid the restriction in
    i.c.completer.IPCompleter.dispatch_custom_completer where
    matches must begin with the text being matched."""

    def __new__(self, text, completion):
        self.text = text
        return str.__new__(self, completion)

    def startswith(self, text):
        if self.text == text:
            return True
        else:
            return super(MonkeyString, self).startswith(text)


class IpydbCompleter(object):
    """Readline completer functions for various ipython commands."""

    restr = re.compile(r'TEXT|VARCHAR.*|CHAR.*')
    renumeric = re.compile(r'FLOAT.*|DECIMAL.*|INT.*'
                           '|DOUBLE.*|FIXED.*|SHORT.*|NUMERIC.*|NUMBER.*')
    redate = re.compile(r'DATE|TIME|DATETIME|TIMESTAMP')

    def __init__(self, get_db):
        """
        Args:
            get_db: callable that will return an
            instance of ipydb.metadata.model.Database
        """
        self.get_db = get_db
        self.commands_completers = {
            'connect': self.connection_nickname,
            'sqlformat': self.sql_format,
            'references': self.table_dot_field,
            'fields': self.table_dot_field,
            'tables': self.table_name,
            'joins': self.table_name,
            'fks': self.table_name,
            'describe': self.table_name,
            'sql': self.sql_statement,
            'runsql': lambda _: None  # delegate to ipython for file match
        }
        self.commands_completers.update(
            zip(SQL_ALIASES, [self.sql_statement] * len(SQL_ALIASES)))

    @property
    def db(self):
        return self.get_db()

    def complete(self, ev):
        """Locate completer for ev.command and call it.
            Args:
                event: see IPython.core.completer
            Returns:
                list of strings which can complete event.symbol
        """
        key = ev.command
        match_assign = reassignment.search(ev.line)
        if match_assign:
            key = match_assign.group(2)
        if ev.command.startswith('%'):
            key = ev.command[1:]
        func = self.commands_completers.get(key)
        if func is None:
            return None
        return func(ev)

    def connection_nickname(self, ev):
        """Return completions for %connect."""
        keys = sorted(getconfigs()[1].keys())
        if not ev.symbol:
            return keys
        matches = match_lists([keys], ev.symbol)
        matches.sort()
        return matches

    def sql_format(self, ev):
        """Return completions for %sql_format."""
        from ipydb.plugin import SQLFORMATS
        if not ev.symbol:
            return SQLFORMATS
        matches = match_lists([SQLFORMATS], ev.symbol)
        matches.sort()
        return matches

    def sql_statement(self, ev):
        """Completions for %sql commands"""
        chunks = ev.line.split()
        if len(chunks) == 2:
            first, second = chunks
            starters = 'select insert'.split()  # TODO: delete, update
            if first in starters and (second in self.db.tablenames() or
                                      self.is_valid_join_expression(second)):
                return self.expand_two_token_sql(ev)
        if '**' in ev.symbol:  # special join syntax t1**t2
            return self.join_shortcut(ev)
        if ev.symbol.count('.') == 1:  # something.other
            return self.dotted_expression(ev, expansion=True)
        # single token, no dot
        matches = match_lists([self.db.tablenames(), self.db.fieldnames(),
                               RESERVED_WORDS], ev.symbol)
        matches.sort()
        return matches

    def table_dot_field(self, ev):
        """completes table.fieldname"""
        if ev.symbol.count('.') == 1:  # something.other
            return self.dotted_expression(ev, expansion=False)
        matches = match_lists([self.db.tablenames()], ev.symbol)
        matches.sort()
        return matches

    def table_name(self, ev):
        matches = match_lists([self.db.tablenames()], ev.symbol)
        matches.sort()
        return matches

    def is_valid_join_expression(self, expr):

        def joining_tables(table):
            for fk in self.db.all_joins(table):
                yield fk.table if table != fk.table else fk.reftable
        if '**' not in expr:
            return False
        tables = expr.split('**')
        valid = True
        while len(tables) > 1:
            tail = tables.pop()
            jointables = joining_tables(tail)
            valid = bool(set(jointables) & set(tables))
            if not valid:
                break
        return valid

    def expand_join_expression(self, expr):
        if not self.is_valid_join_expression(expr):
            log.debug('%s is not a valid join expr', expr)
            return expr
        tables = expr.split('**')
        ret = ''
        while len(tables) > 1:
            tail = tables.pop()
            # try to join to the other tables:
            for tbl in reversed(tables):
                joins = self.db.get_joins(tbl, tail)
                if joins:
                    join = next(iter(joins))  # XXX: take a punt
                    joinstr = 'inner join %s on ' % (tail)
                    sep = ''
                    for idx, col in enumerate(join.columns):
                        joinstr += sep + '%s.%s = %s.%s' % (
                            join.table, col, join.reftable,
                            join.refcolumns[idx])
                        sep = ' and '
                    ret = joinstr + ' ' + ret
                    break
        ret = tables[0] + ' ' + ret
        return ret

    def join_shortcut(self, ev):
        matches = []

        def _all_joining_tables(tables):
            ret = set()
            for tablename in tables:
                for fk in self.db.all_joins(tablename):
                    tgt = fk.reftable if fk.table == tablename else fk.table
                    ret.add(tgt)
            return ret

        if ev.symbol.endswith('**'):  # incomplete stmt: t1**t2**<tab>
            for t in _all_joining_tables(ev.symbol.split('**')):
                matches.append(MonkeyString(ev.symbol, ev.symbol + t))
        else:
            joinexpr = self.expand_join_expression(ev.symbol)
            if joinexpr != ev.symbol:  # expand succeeded
                return [MonkeyString(ev.symbol, joinexpr)]
            # assume that end token is partial table name:
            bits = ev.symbol.split('**')
            toke = bits.pop()
            start = '**'.join(bits)
            all_joins = _all_joining_tables(bits)
            matches = [MonkeyString(ev.symbol,  start + '**' + t)
                       for t in all_joins if t.startswith(toke)]
        matches.sort()
        return matches

    def dotted_expression(self, ev, expansion=True):
        """Return completions for head.tail<tab>"""
        head, tail = ev.symbol.split('.')
        if expansion and head in self.db.tablenames() and tail == '*':
            # tablename.*<tab> -> expand all names
            matches = self.db.fieldnames(table=head, dotted=True)
            return [MonkeyString(ev.symbol, ', '.join(sorted(matches)))]
        matches = match_lists([self.db.fieldnames(dotted=True)], ev.symbol)
        if not len(matches):  # head could be a table alias TODO: parse these.
            if tail == '':
                fields = map(lambda word: head + '.' + word,
                             self.db.fieldnames())
                matches.extend(fields)
            else:
                match_lists([self.db.fieldnames()], tail, matches.append,
                            matches.sort)
        matches.sort()
        return matches

    def expand_two_token_sql(self, ev):
        """Return special expansions for 'select tablename<tab>'
        and for insert 'tablename<tab>'"""
        first, tablename = ev.line.split()
        if first == 'select':
            colstr = ', '.join('%s.*' % t for t in tablename.split('**'))
            tablename = self.expand_join_expression(tablename)
            return [MonkeyString(ev.symbol, '%s from %s' %
                    (colstr, tablename))]
        elif first == 'insert':
            ins = self.db.insert_statement(tablename)
            return [MonkeyString(ev.symbol, ins.lstrip('insert'))]
