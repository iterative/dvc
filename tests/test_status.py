import os

from dvc.main import main

from tests.basic_env import TestDvc
from mock import patch


class TestStatus(TestDvc):
    def test_quiet(self):
        self.dvc.add(self.FOO)

        ret = main(["status", "--quiet"])
        self.assertEqual(ret, 0)

        os.remove(self.FOO)
        os.rename(self.BAR, self.FOO)

        ret = main(["status", "--quiet"])
        self.assertEqual(ret, 1)

    @patch("dvc.repo.status._cloud_status", return_value=True)
    def test_implied_cloud(self, mock_status):
        main(["status", "--remote", "something"])
        mock_status.assert_called()
