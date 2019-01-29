import configobj

from dvc.main import main
from dvc.command.config import CmdConfig
from tests.basic_env import TestDvc


class TestConfigCLI(TestDvc):
    def _contains(self, section, field, value, local=False):
        if local:
            fname = self.dvc.config.config_local_file
        else:
            fname = self.dvc.config.config_file

        config = configobj.ConfigObj(fname)
        if section not in config.keys():
            return False

        if field not in config[section].keys():
            return False

        if config[section][field] != value:
            return False

        return True

    def test_root(self):
        ret = main(["root"])
        self.assertEqual(ret, 0)

        # NOTE: check that `dvc root` is not blocked with dvc lock
        with self.dvc.lock:
            ret = main(["root"])
        self.assertEqual(ret, 0)

    def _do_test(self, local=False):
        section = "setsection"
        field = "setfield"
        section_field = "{}.{}".format(section, field)
        value = "setvalue"
        newvalue = "setnewvalue"

        base = ["config"]
        if local:
            base.append("--local")

        ret = main(base + [section_field, value])
        self.assertEqual(ret, 0)
        self.assertTrue(self._contains(section, field, value, local))

        ret = main(base + [section_field])
        self.assertEqual(ret, 0)

        ret = main(base + [section_field, newvalue])
        self.assertEqual(ret, 0)
        self.assertTrue(self._contains(section, field, newvalue, local))
        self.assertFalse(self._contains(section, field, value, local))

        ret = main(base + [section_field, "--unset"])
        self.assertEqual(ret, 0)
        self.assertFalse(self._contains(section, field, value, local))

    def test(self):
        self._do_test(False)

    def test_local(self):
        self._do_test(True)

    def test_non_existing(self):
        # FIXME check stdout/err

        ret = main(["config", "non_existing_section.field"])
        self.assertEqual(ret, 1)

        ret = main(["config", "global.non_existing_field"])
        self.assertEqual(ret, 1)

        ret = main(["config", "non_existing_section.field", "-u"])
        self.assertEqual(ret, 1)

        ret = main(["config", "global.non_existing_field", "-u"])
        self.assertEqual(ret, 1)

        ret = main(["config", "core.remote", "myremote"])
        self.assertEqual(ret, 0)

        ret = main(["config", "core.non_existing_field", "-u"])
        self.assertEqual(ret, 1)

    def test_failed_write(self):
        class A(object):
            system = False
            glob = False
            local = False
            name = "core.remote"
            value = "myremote"
            unset = False

        args = A()
        cmd = CmdConfig(args)

        cmd.configobj.write = None
        ret = cmd.run()
        self.assertNotEqual(ret, 0)
