# -*- coding: utf-8 -*-

"""
This module provides functionality for readline-style
tab-completion of SQL statements and other ipydb commands.
"""
import itertools
import re

from sqlalchemy.sql.compiler import RESERVED_WORDS

from ipydb.engine import getconfigs
from ipydb.magic import SQL_ALIASES


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
                print 'complete: sym=[%s] line=[%s] tuc=[%s]' % (
                    event.symbol, event.line, event.text_until_cursor)
            completions = sqlplugin.completer.complete(event)
            if sqlplugin.debug:
                print 'completions:', completions
            return completions
    except Exception, e:
        print repr(e)
        if sqlplugin and sqlplugin.debug:
            import traceback
            traceback.print_exc()
    return None


def match_lists(lists, text, appendfunc=None):
    """Helper to substring-match text in a list-of-lists."""
    n = len(text)
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
                           '|DOUBLE.*|FIXED.*|SHORT.*')
    redate = re.compile(r'DATE|TIME|DATETIME|TIMESTAMP')

    def __init__(self, ipydb):
        """Constructor.

        Args:
            ipydb: instance of ipydb.plugin.SqlPlugin
        """
        self.ipydb = ipydb
        self.commands_completers = {
            'connect': self.connection_nickname,
            'sqlformat': self.sql_format,
            'what_references': self.sql_statement,
            'show_fields': self.sql_statement,
            'show_tables': self.table_name,
            'show_joins': self.table_name,
            'show_fks': self.table_name,
            'describe': self.table_name,
            'sql': self.sql_statement,
            'runsql': lambda _: None  # delegate to ipython for file match
        }
        self.commands_completers.update(
            zip(SQL_ALIASES, [self.sql_statement] * len(SQL_ALIASES)))

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
        return match_lists([keys], ev.symbol)

    def sql_format(self, ev):
        """Return completions for %sql_format."""
        formats = self.ipydb.sqlformats
        if not ev.symbol:
            return formats
        return match_lists([formats], ev.symbol)

    def sql_statement(self, ev):
        """Completions for %sql commands"""
        metadata = self.ipydb.comp_data
        chunks = ev.line.split()
        if len(chunks) == 2:
            first, second = chunks
            starters = 'select insert'.split()  # TODO: delete, update
            if first in starters and (second in metadata.tables or
                                      self.is_valid_join_expression(second)):
                return self.expand_two_token_sql(ev)
        if ev.symbol.count('.') == 1:  # something.other
            return self.dotted_expression(ev)
        if '**' in ev.symbol:  # special join syntax t1**t2
            return self.join_shortcut(ev)
        # simple single-token completion: foo<tab>
        return match_lists([metadata.tables, metadata.fields, RESERVED_WORDS],
                           ev.symbol)

    def table_name(self, ev):
        metadata = self.ipydb.comp_data
        return match_lists([metadata.tables], ev.symbol)

    def is_valid_join_expression(self, expr):
        metadata = self.ipydb.comp_data
        if '**' not in expr:
            return False
        tables = expr.split('**')
        valid = True
        while len(tables) > 1:
            tail = tables.pop()
            jointables = metadata.tables_referencing(tail)
            valid = bool(set(jointables) & set(tables))
            if not valid:
                break
        return valid

    def expand_join_expression(self, expr):
        metadata = self.ipydb.comp_data
        if not self.is_valid_join_expression(expr):
            return expr
        tables = expr.split('**')
        ret = ''
        while len(tables) > 1:
            tail = tables.pop()
            # try to join to the other tables:
            for tbl in reversed(tables):
                joins = metadata.get_joins(tbl, tail)
                if joins:
                    join = joins[0]  # XXX: take a punt
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
        metadata = self.ipydb.comp_data

        def _all_joining_tables(tables):
            ret = set()
            for tablename in tables:
                for reftable in metadata.tables_referencing(tablename):
                    ret.add(reftable)
            return ret

        if ev.symbol.endswith('**'):  # incomplete stmt: t1**t2**<tab>
            matches = []
            for t in _all_joining_tables(ev.symbol.split('**')):
                matches.append(MonkeyString(ev.symbol, ev.symbol + t))
            return matches
        else:
            joinexpr = self.expand_join_expression(ev.symbol)
            if joinexpr != ev.symbol:  # expand succeeded
                return [MonkeyString(ev.symbol, joinexpr)]
            # assume that end token is partial table name:
            bits = ev.symbol.split('**')
            toke = bits.pop()
            start = '**'.join(bits)
            all_joins = _all_joining_tables(bits)
            return [MonkeyString(ev.symbol,  start + '**' + t)
                    for t in all_joins if t.startswith(toke)]

        return []

    def dotted_expression(self, ev, expansion=True):
        """Return completions for head.tail<tab>"""
        metadata = self.ipydb.comp_data
        head, tail = ev.symbol.split('.')
        if expansion and head in metadata.tables and tail == '*':
            # tablename.*<tab> -> expand all names
            matches = metadata
            return [MonkeyString(ev.symbol,
                    ', '.join(sorted(metadata.get_dottedfields(table=head))))]
        matches = match_lists([metadata.dottedfields], ev.symbol)
        if not len(matches):
            if tail == '':
                fields = map(lambda word: head + '.' + word, metadata.fields)
                matches.extend(fields)
            else:
                match_lists([metadata.fields], tail, matches.append)
        return matches

    def expand_two_token_sql(self, ev):
        """Return special expansions for 'select tablename<tab>'
        and for insert 'tablename<tab>'"""
        metadata = self.ipydb.comp_data
        first, tablename = ev.line.split()
        if first == 'select':
            colstr = ', '.join('%s.*' % t for t in tablename.split('**'))
            tablename = self.expand_join_expression(tablename)
            return [MonkeyString(ev.symbol, '%s from %s' %
                    (colstr, tablename))]
        elif first == 'insert':
            # XXX: make sure that tablename is a tablename here!
            # and not a t1**t2**t3
            cols = metadata.get_fields(table=tablename)
            colstr = ', '.join(sorted(cols))
            dcols = metadata.get_dottedfields(table=tablename)
            deflt = []
            types = metadata.types
            for dc in sorted(dcols):
                default_value = self.default_value_for_type(types[dc])
                deflt.append(default_value)
            return [MonkeyString(ev.symbol,
                    'into %s (%s) values (%s)' %
                    (tablename, colstr, ', '.join(deflt)))]

    def default_value_for_type(self, typ):
        """Returns a default value which can be used for the SQL type typ."""
        value = ''
        if self.redate.search(typ):
            value = "''"  # XXX: now() or something?
        elif self.restr.search(typ):
            value = "''"
        elif self.renumeric.search(typ):
            value = "0"
        return value
