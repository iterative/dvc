import configobj

from dvc.main import main
from tests.basic_env import TestDvc


class TestConfigCLI(TestDvc):
    def _contains(self, section, field, value):
        config = configobj.ConfigObj(self.dvc.config.config_file)
        if section not in config.keys():
            return False

        if field not in config[section].keys():
            return False

        if config[section][field] != value:
            return False

        return True

    def test(self):
        section = 'setsection'
        field = 'setfield'
        section_field = '{}.{}'.format(section, field)
        value = 'setvalue'
        newvalue = 'setnewvalue'

        ret = main(['config', section_field, value])
        self.assertEqual(ret, 0)
        self.assertTrue(self._contains(section, field, value))

        ret = main(['config', section_field])
        self.assertEqual(ret, 0)

        ret = main(['config', section_field, newvalue])
        self.assertEqual(ret, 0)
        self.assertTrue(self._contains(section, field, newvalue))
        self.assertFalse(self._contains(section, field, value))

        ret = main(['config', section_field, '--unset'])
        self.assertEqual(ret, 0)
        self.assertFalse(self._contains(section, field, value))

    def test_non_existing(self):
        #FIXME check stdout/err

        ret = main(['config', 'non_existing_section.field'])
        self.assertEqual(ret, 1)

        ret = main(['config', 'global.non_existing_field'])
        self.assertEqual(ret, 1)

        ret = main(['config', 'non_existing_section.field', '-u'])
        self.assertEqual(ret, 1)

        ret = main(['config', 'global.non_existing_field', '-u'])
        self.assertEqual(ret, 1)
