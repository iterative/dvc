import os
from uuid import uuid4

from dvc.main import main

from tests.basic_env import TestDvc


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
