import unittest

import nose.tools as nt
import mock

from ipydb import plugin


class SqlPluginTest(unittest.TestCase):

    def setUp(self):
        self.md_accessor = mock.patch('ipydb.metadata.MetaDataAccessor')
        plugin.SqlPlugin.metadata_accessor = self.md_accessor
        self.ipython = mock.MagicMock()
        self.ip = plugin.SqlPlugin(shell=self.ipython)

    def test_something(self):
        pass
