# -*- coding: utf-8 -*-

"""
This module provides functionality for readline-style
tab-completion of SQL statements and other ipydb commands.
"""
import itertools
import re

from sqlalchemy.sql.compiler import RESERVED_WORDS

from ipydb import PLUGIN_NAME
from ipydb.engine import getconfigs
from ipydb.magic import SQL_ALIASES


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
    try:
        sqlplugin = self.plugin_manager.get_plugin(PLUGIN_NAME)
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
            'what_references': self.dotted_expression,
            'show_fields': self.sql_statement,
            'show_tables': self.sql_statement,
            'sql': self.sql_statement
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
        if ev.command.startswith('%'):
            key = ev.command[1:]
        func = self.commands_completers.get(key)
        if func is None:
            print "Warning: no completer for:", ev.command
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
        metadata = self.ipydb.get_completion_data()
        chunks = ev.line.split()
        if len(chunks) == 2:
            first, second = chunks
            starters = 'select insert'.split()  # TODO: delete, update
            if first in starters and second in metadata.tables:
                return self.expand_two_token_sql(ev)
        if ev.symbol.count('.') == 1:  # something.other
            return self.dotted_expression(ev)
        # simple single-token completion: foo<tab>
        return match_lists([metadata.tables, metadata.fields, RESERVED_WORDS],
                           ev.symbol)

    def dotted_expression(self, ev, expansion=True):
        """Return completions for head.tail<tab>"""
        metadata = self.ipydb.get_completion_data()
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
        metadata = self.ipydb.get_completion_data()
        first, tablename = ev.line.split()
        cols = metadata.get_fields(table=tablename)
        colstr = ', '.join(sorted(cols))
        if first == 'select':
            return [MonkeyString(ev.symbol, '%s from %s order by %s' %
                    (colstr, tablename, cols[0]))]
        elif first == 'insert':
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
            value = '""'  # XXX: now() or something?
        elif self.restr.search(typ):
            value = '""'
        elif self.renumeric.search(typ):
            value = '0'
        return value
