from dvc.main import main

from tests.basic_env import TestDvc


class TestRemote(TestDvc):
    def test(self):
        remotes = ['a', 'b', 'c']

        self.assertEqual(main(['remote', 'list']), 0)
        self.assertNotEqual(main(['remote', 'remove', remotes[0]]), 0)

        for r in remotes:
            self.assertEqual(main(['remote', 'add', r, 's3://bucket/name']), 0)

        self.assertEqual(main(['remote', 'list']), 0)

        self.assertEqual(main(['remote', 'remove', remotes[0]]), 0)
        self.assertEqual(main(['remote', 'modify', remotes[0], 'option', 'value']), 0)

        self.assertEqual(main(['remote', 'list']), 0)
