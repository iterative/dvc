from unittest import TestCase

import mock

from dvc.repo import Repo
from dvc.repo.scm_context import scm_context
from dvc.scm import NoSCM


class TestScmContext(TestCase):
    def setUp(self):
        self.repo_mock = mock.Mock(spec=Repo)
        self.scm_mock = mock.Mock(spec=NoSCM)
        self.repo_mock.scm = self.scm_mock

    def test_should_successfully_perform_method(self):
        method = mock.Mock()
        wrapped = scm_context(method)

        self.repo_mock.configure_mock(config={})
        wrapped(self.repo_mock)

        self.assertEqual(1, method.call_count)
        self.assertEqual(1, self.scm_mock.reset_ignores.call_count)
        self.assertEqual(1, self.scm_mock.remind_to_track.call_count)

        self.assertEqual(0, self.scm_mock.cleanup_ignores.call_count)

    def test_should_check_autostage(self):
        method = mock.Mock()
        wrapped = scm_context(method)

        config_autostage_attrs = {"config": {"core": {"autostage": True}}}
        self.repo_mock.configure_mock(**config_autostage_attrs)
        wrapped(self.repo_mock)

        self.assertEqual(1, method.call_count)
        self.assertEqual(1, self.scm_mock.reset_ignores.call_count)
        self.assertEqual(1, self.scm_mock.track_changed_files.call_count)

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


def test_remind_to_track(scm, caplog):
    scm.files_to_track = ["fname with spaces.txt", "тест", "foo"]
    scm.remind_to_track()
    assert "git add 'fname with spaces.txt' 'тест' foo" in caplog.text
