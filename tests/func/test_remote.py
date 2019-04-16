import os
import configobj

from dvc.main import main
from dvc.command.remote import CmdRemoteAdd
from dvc.config import Config

from tests.basic_env import TestDvc
from tests.func.test_data_cloud import get_local_storagepath


class TestRemote(TestDvc):
    def test(self):
        remotes = ["a", "b", "c"]

        self.assertEqual(main(["remote", "list"]), 0)
        self.assertNotEqual(main(["remote", "remove", remotes[0]]), 0)

        for r in remotes:
            self.assertEqual(
                main(["remote", "add", "--default", r, "s3://bucket/name"]), 0
            )

        self.assertEqual(main(["remote", "list"]), 0)

        self.assertEqual(main(["remote", "remove", remotes[0]]), 0)
        self.assertEqual(
            main(["remote", "modify", remotes[0], "option", "value"]), 0
        )

        self.assertEqual(main(["remote", "list"]), 0)

    def test_failed_write(self):
        class A(object):
            system = False
            glob = False
            local = False
            name = "myremote"
            url = "s3://remote"
            unset = False

        args = A()
        cmd = CmdRemoteAdd(args)

        cmd.configobj.write = None
        ret = cmd.run()
        self.assertNotEqual(ret, 0)

    def test_relative_path(self):
        dname = os.path.join("..", "path", "to", "dir")
        ret = main(["remote", "add", "mylocal", dname])
        self.assertEqual(ret, 0)

        # NOTE: we are in the repo's root and config is in .dvc/, so
        # dir path written to config should be just one level above.
        rel = os.path.join("..", dname)
        config = configobj.ConfigObj(self.dvc.config.config_file)
        self.assertEqual(config['remote "mylocal"']["url"], rel)

    def test_overwrite(self):
        remote_name = "a"
        remote_url = "s3://bucket/name"
        self.assertEqual(main(["remote", "add", remote_name, remote_url]), 0)
        self.assertEqual(main(["remote", "add", remote_name, remote_url]), 1)
        self.assertEqual(
            main(["remote", "add", "-f", remote_name, remote_url]), 0
        )

    def test_referencing_other_remotes(self):
        assert main(["remote", "add", "foo", "ssh://localhost/"]) == 0
        assert main(["remote", "add", "bar", "remote://foo/dvc-storage"]) == 0

        config = configobj.ConfigObj(self.dvc.config.config_file)

        assert config['remote "bar"']["url"] == "remote://foo/dvc-storage"


class TestRemoteRemoveDefault(TestDvc):
    def test(self):
        remote = "mys3"
        self.assertEqual(
            main(["remote", "add", "--default", remote, "s3://bucket/name"]), 0
        )
        self.assertEqual(
            main(["remote", "modify", remote, "profile", "default"]), 0
        )
        self.assertEqual(main(["config", "--local", "core.remote", remote]), 0)

        config = configobj.ConfigObj(
            os.path.join(self.dvc.dvc_dir, Config.CONFIG)
        )
        local_config = configobj.ConfigObj(
            os.path.join(self.dvc.dvc_dir, Config.CONFIG_LOCAL)
        )
        self.assertEqual(
            config[Config.SECTION_CORE][Config.SECTION_CORE_REMOTE], remote
        )
        self.assertEqual(
            local_config[Config.SECTION_CORE][Config.SECTION_CORE_REMOTE],
            remote,
        )

        self.assertEqual(main(["remote", "remove", remote]), 0)
        config = configobj.ConfigObj(
            os.path.join(self.dvc.dvc_dir, Config.CONFIG)
        )
        local_config = configobj.ConfigObj(
            os.path.join(self.dvc.dvc_dir, Config.CONFIG_LOCAL)
        )
        section = Config.SECTION_REMOTE_FMT.format(remote)
        self.assertTrue(section not in config.keys())

        core = config.get(Config.SECTION_CORE, None)
        if core is not None:
            self.assertTrue(Config.SECTION_CORE_REMOTE not in core.keys())

        core = local_config.get(Config.SECTION_CORE, None)
        if core is not None:
            self.assertTrue(Config.SECTION_CORE_REMOTE not in core.keys())


class TestRemoteRemove(TestDvc):
    def test(self):
        ret = main(["config", "core.jobs", "1"])
        self.assertEqual(ret, 0)

        remote = "mys3"
        ret = main(["remote", "add", remote, "s3://bucket/name"])
        self.assertEqual(ret, 0)

        ret = main(["remote", "remove", remote])
        self.assertEqual(ret, 0)


class TestRemoteDefault(TestDvc):
    def test(self):
        remote = "mys3"
        ret = main(["remote", "add", "mys3", "s3://bucket/path"])
        self.assertEqual(ret, 0)

        ret = main(["remote", "default", "mys3"])
        self.assertEqual(ret, 0)
        config_file = os.path.join(self.dvc.dvc_dir, Config.CONFIG)
        config = configobj.ConfigObj(config_file)
        default = config[Config.SECTION_CORE][Config.SECTION_CORE_REMOTE]
        self.assertEqual(default, remote)

        ret = main(["remote", "default", "--unset"])
        self.assertEqual(ret, 0)
        config = configobj.ConfigObj(config_file)
        core = config.get(Config.SECTION_CORE)
        if core is not None:
            default = core.get(Config.SECTION_CORE_REMOTE)
        else:
            default = None
        self.assertEqual(default, None)


class TestRemoteShouldHandleUppercaseRemoteName(TestDvc):
    upper_case_remote_name = "UPPERCASEREMOTE"

    def test(self):
        remote_url = get_local_storagepath()
        ret = main(["remote", "add", self.upper_case_remote_name, remote_url])
        self.assertEqual(ret, 0)

        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        ret = main(["push", "-r", self.upper_case_remote_name])
        self.assertEqual(ret, 0)
