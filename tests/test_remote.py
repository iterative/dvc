import os
import configobj

from dvc.main import main
from dvc.project import Project
from dvc.command.remote import CmdRemoteAdd
from dvc.exceptions import UnsupportedRemoteError
from dvc.remote import Remote
from dvc.config import Config

from tests.basic_env import TestDvc


class TestRemote(TestDvc):
    def test(self):
        remotes = ['a', 'b', 'c']

        self.assertEqual(main(['remote', 'list']), 0)
        self.assertNotEqual(main(['remote', 'remove', remotes[0]]), 0)

        for r in remotes:
            self.assertEqual(main(['remote', 'add', '--default', r, 's3://bucket/name']), 0)

        self.assertEqual(main(['remote', 'list']), 0)

        self.assertEqual(main(['remote', 'remove', remotes[0]]), 0)
        self.assertEqual(main(['remote', 'modify', remotes[0], 'option', 'value']), 0)

        self.assertEqual(main(['remote', 'list']), 0)

    def test_failed_write(self):
        class A(object):
            local = False
            name = 'myremote'
            url = 's3://remote'
            unset = False

        args = A()
        cmd = CmdRemoteAdd(args)

        cmd.configobj.write = None
        ret = cmd.run()
        self.assertNotEqual(ret, 0)

    def test_unsupported(self):
        with self.assertRaises(UnsupportedRemoteError):
            Remote(self.dvc, {Config.SECTION_REMOTE_URL: 'unsupported://url'})


class TestRemoteRemoveDefault(TestDvc):
    def test(self):
        remote = 'mys3'
        self.assertEqual(main(['remote', 'add', '--default', remote, 's3://bucket/name']), 0)
        self.assertEqual(main(['remote', 'modify', remote, 'profile', 'default']), 0)
        self.assertEqual(main(['config', '--local', 'core.remote', remote]), 0)

        config = configobj.ConfigObj(os.path.join(self.dvc.dvc_dir, Config.CONFIG))
        local_config = configobj.ConfigObj(os.path.join(self.dvc.dvc_dir, Config.CONFIG_LOCAL))
        self.assertEqual(config[Config.SECTION_CORE][Config.SECTION_CORE_REMOTE], remote)
        self.assertEqual(local_config[Config.SECTION_CORE][Config.SECTION_CORE_REMOTE], remote)

        self.assertEqual(main(['remote', 'remove', remote]), 0)
        config = configobj.ConfigObj(os.path.join(self.dvc.dvc_dir, Config.CONFIG))
        local_config = configobj.ConfigObj(os.path.join(self.dvc.dvc_dir, Config.CONFIG_LOCAL))
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
        ret = main(['config', 'core.jobs', '1'])
        self.assertEqual(ret, 0)

        remote = 'mys3'
        ret = main(['remote', 'add', remote, 's3://bucket/name'])
        self.assertEqual(ret, 0)

        ret = main(['remote', 'remove', remote])
        self.assertEqual(ret, 0)
