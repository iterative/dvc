import mock

from unittest import TestCase

from dvc.main import main


class TestCli(TestCase):
    @mock.patch("dvc.main.Analytics", autospec=True)
    @mock.patch("dvc.cli.run.CmdRun", autospec=True)
    def test_file_name(self, cmd_mock, _):
        cmd_mock.return_value.run_cmd.return_value = 0
        ret = main(["run", "-f", "test/out.dvc", "echo"])
        self.assertEqual(ret, 0)
        cmd_mock.assert_called_once()
        args, kwargs = cmd_mock.call_args_list[0]
        self.assertEqual(len(args), 1)
        self.assertEqual(len(kwargs), 0)
        self.assertEqual(args[0].file, "test/out.dvc")
        self.assertSequenceEqual(args[0].command, ["echo"])
        cmd_mock.return_value.run_cmd.assert_called_once()

    @mock.patch("dvc.main.Analytics", autospec=True)
    @mock.patch("dvc.cli.run.CmdRun", autospec=True)
    def test_file_name_default(self, cmd_mock, _):
        cmd_mock.return_value.run_cmd.return_value = 0
        ret = main(["run", "-d", "foo", "echo"])
        self.assertEqual(ret, 0)
        cmd_mock.assert_called_once()
        args, kwargs = cmd_mock.call_args_list[0]
        self.assertEqual(len(args), 1)
        self.assertEqual(len(kwargs), 0)
        self.assertIsNone(args[0].file)
        self.assertSequenceEqual(args[0].command, ["echo"])
        cmd_mock.return_value.run_cmd.assert_called_once()

    @mock.patch("dvc.main.Analytics", autospec=True)
    @mock.patch("dvc.cli.run.CmdRun", autospec=True)
    def test_wdir(self, cmd_mock, _):
        cmd_mock.return_value.run_cmd.return_value = 0
        ret = main(["run", "--wdir", "test/", "echo"])
        self.assertEqual(ret, 0)
        cmd_mock.assert_called_once()
        args, kwargs = cmd_mock.call_args_list[0]
        self.assertEqual(len(args), 1)
        self.assertEqual(len(kwargs), 0)
        self.assertEqual(args[0].wdir, "test/")
        self.assertSequenceEqual(args[0].command, ["echo"])
        cmd_mock.return_value.run_cmd.assert_called_once()

    @mock.patch("dvc.main.Analytics", autospec=True)
    @mock.patch("dvc.cli.run.CmdRun", autospec=True)
    def test_wdir_default(self, cmd_mock, _):
        cmd_mock.return_value.run_cmd.return_value = 0
        ret = main(["run", "echo"])
        self.assertEqual(ret, 0)
        cmd_mock.assert_called_once()
        args, kwargs = cmd_mock.call_args_list[0]
        self.assertEqual(len(args), 1)
        self.assertEqual(len(kwargs), 0)
        self.assertEqual(args[0].wdir, None)
        self.assertSequenceEqual(args[0].command, ["echo"])
        cmd_mock.return_value.run_cmd.assert_called_once()
