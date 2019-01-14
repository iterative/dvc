import os
from uuid import uuid4

from dvc import logger
from dvc.exceptions import DvcException
from dvc.main import main
from mock import patch
from tests.basic_env import TestDvc

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


class TestCmdImport(TestDvc):
    def test(self):
        ret = main(['import', self.FOO, 'import'])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.exists('import.dvc'))

        ret = main(['import', 'non-existing-file', 'import'])
        self.assertNotEqual(ret, 0)

    def test_unsupported(self):
        ret = main(['import', 'unsupported://path', 'import_unsupported'])
        self.assertNotEqual(ret, 0)


class TestDefaultOutput(TestDvc):
    def test(self):
        tmpdir = self.mkdtemp()
        filename = str(uuid4())
        tmpfile = os.path.join(tmpdir, filename)

        with open(tmpfile, 'w') as fd:
            fd.write('content')

        ret = main(['import', tmpfile])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.exists(filename))
        self.assertEqual(open(filename).read(), 'content')


class TestFailedImportMessage(TestDvc):
    color_patch = patch.object(logger, 'colorize', new=lambda x, color='': x)

    def test(self):
        self.color_patch.start()
        self._test()
        self.color_patch.stop()

    @patch('dvc.command.imp.urlparse')
    def _test(self, imp_urlparse_patch):
        logger.logger.handlers[1].stream = StringIO()
        page_address = 'http://somesite.com/file_name'

        def dvc_exception(*args, **kwargs):
            raise DvcException('message')

        imp_urlparse_patch.side_effect = dvc_exception
        main(['import', page_address])
        self.assertIn('Error: failed to import '
                      'http://somesite.com/file_name. Download it '
                      'manually and add it with `dvc add` command',
                      logger.logger.handlers[1].stream.getvalue())
