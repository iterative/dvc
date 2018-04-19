import os
import json

from dvc.main import main
from tests.basic_env import TestDvc

class TestMetrics(TestDvc):
    def setUp(self):
        super(TestMetrics, self).setUp()
        for branch in ['foo', 'bar', 'baz']:
            self.dvc.scm.checkout(branch, create_new=True)

            with open('metric', 'w+') as fd:
                fd.write(branch)

            with open('metric_json', 'w+') as fd:
                json.dump({'branch': branch}, fd)

            with open('metric_tsv', 'w+') as fd:
                fd.write(branch)

            with open('metric_htsv', 'w+') as fd:
                fd.write('branch\n')
                fd.write(branch)

            self.dvc.scm.add(['metric', 'metric_json', 'metric_tsv', 'metric_htsv'])
            self.dvc.scm.commit('metric')

        self.dvc.scm.checkout('master')

    def test(self):
        ret = self.dvc.metrics('metric')
        self.assertEqual(len(ret), 4)
        self.assertTrue(ret['master'] == None)
        self.assertTrue(ret['foo'] == 'foo')
        self.assertTrue(ret['bar'] == 'bar')
        self.assertTrue(ret['baz'] == 'baz')

        ret = self.dvc.metrics('metric_json', json_path='branch')
        self.assertEqual(len(ret), 4)
        self.assertTrue(ret['master'] == None)
        self.assertTrue(ret['foo'] == ['foo'])
        self.assertTrue(ret['bar'] == ['bar'])
        self.assertTrue(ret['baz'] == ['baz'])

        ret = self.dvc.metrics('metric_tsv', tsv_path='0,0')
        self.assertEqual(len(ret), 4)
        self.assertTrue(ret['master'] == None)
        self.assertTrue(ret['foo'] == ['foo'])
        self.assertTrue(ret['bar'] == ['bar'])
        self.assertTrue(ret['baz'] == ['baz'])

        ret = self.dvc.metrics('metric_htsv', htsv_path='branch,0')
        self.assertEqual(len(ret), 4)
        self.assertTrue(ret['master'] == None)
        self.assertTrue(ret['foo'] == ['foo'])
        self.assertTrue(ret['bar'] == ['bar'])
        self.assertTrue(ret['baz'] == ['baz'])

    def test_cli(self):
        #FIXME check output
        ret = main(['metrics', 'metric'])
        self.assertEqual(ret, 0)

        ret = main(['metrics', 'metric_json', '--json-path', 'branch'])
        self.assertEqual(ret, 0)

        ret = main(['metrics', 'metric_tsv', '--tsv-path', '0,0'])
        self.assertEqual(ret, 0)

        ret = main(['metrics', 'metric_htsv', '--htsv-path', 'branch,0'])
        self.assertEqual(ret, 0)
