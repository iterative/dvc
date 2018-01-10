import os
import pygraphviz

from dvc.main import main

from tests.test_repro import TestRepro


class TestShowPipeline(TestRepro):
    def test_show(self):
        ret = main(['show',
                    'pipeline'])

        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile('pipeline_all.dot'))
        self.assertTrue(os.path.isfile('pipeline_all.jpeg'))

    def test_target(self):
        base = 'pipeline_' + os.path.basename(self.file1_stage)
        ret = main(['show',
                    'pipeline',
                    self.file1_stage])

        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(base + '.dot'))
        self.assertTrue(os.path.isfile(base + '.jpeg'))

    def test_non_existing_target(self):
        ret = main(['show',
                    'pipeline',
                    'non-existing-target'])

        self.assertNotEqual(ret, 0)
