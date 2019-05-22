from dvc.repo import Repo
from dvc.repo.scm_context import scm_context
from dvc.scm import NoSCM
from unittest import TestCase
import mock


class TestScmContext(TestCase):
    def setUp(self):
        self.repo_mock = mock.Mock(spec=Repo)
        self.scm_mock = mock.Mock(spec=NoSCM)
        self.repo_mock.scm = self.scm_mock

    def test_should_successfully_perform_method(self):
        method = mock.Mock()
        wrapped = scm_context(method)

        wrapped(self.repo_mock)

        self.assertEqual(1, method.call_count)
        self.assertEqual(1, self.scm_mock.reset_ignores.call_count)
        self.assertEqual(1, self.scm_mock.remind_to_track.call_count)

        self.assertEqual(0, self.scm_mock.cleanup_ignores.call_count)

    def test_should_throw_and_cleanup(self):
        method = mock.Mock(side_effect=Exception("some problem"))
        wrapped = scm_context(method)

        with self.assertRaises(Exception):
            wrapped(self.repo_mock)

        self.assertEqual(1, method.call_count)
        self.assertEqual(1, self.scm_mock.cleanup_ignores.call_count)

        self.assertEqual(0, self.scm_mock.reset_ignores.call_count)
        self.assertEqual(0, self.scm_mock.remind_to_track.call_count)
