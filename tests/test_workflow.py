from unittest import TestCase

from dvc.graph.workflow import Workflow
from dvc.graph.commit import Commit


class TestWorkflow(TestCase):
    def setUp(self):
        self._commit4 = Commit('4', '3', 'name1', 'today', 'comment4')
        self._commit3 = Commit('3', '2', 'name1', 'today', 'DVC repro-run ...')
        self._commit2 = Commit('2', '1', 'name1', 'today', 'DVC repro-run ...')
        self._commit1 = Commit('1', '', 'name1', 'today', 'comment1')

    def commits_basic_test(self):
        self.assertFalse(self._commit1.is_repro)
        self.assertTrue(self._commit2.is_repro)
        self.assertTrue(self._commit3.is_repro)
        self.assertFalse(self._commit4.is_repro)
        pass

    def workflow_basic_test(self):
        wf = Workflow('', '')
        wf.add_commit(self._commit1)
        wf.add_commit(self._commit2)
        wf.add_commit(self._commit3)
        wf.add_commit(self._commit4)

        self.assertEqual(len(wf._commits), 4)

        self.assertEqual(wf._commits['1'].text, self._commit1._comment + '\n' + self._commit1.hash)
        self.assertEqual(wf._commits['2'].text, self._commit2._comment + '\n' + self._commit2.hash)
        self.assertEqual(wf._commits['3'].text, self._commit3._comment + '\n' + self._commit3.hash)
        self.assertEqual(wf._commits['4'].text, self._commit4._comment + '\n' + self._commit4.hash)
        pass

    def collapse_test(self):
        wf = Workflow('', '')
        wf.add_commit(self._commit1)
        wf.add_commit(self._commit2)
        wf.add_commit(self._commit3)
        wf.add_commit(self._commit4)

        wf.collapse_repro_commits()

        self.assertEqual(len(wf._commits), 3)
        self.assertEqual(wf._commits[self._commit1.hash].text, self._commit1._comment + '\n' + self._commit1.hash)
        self.assertEqual(wf._commits[self._commit3.hash].text, Commit.COLLAPSED_TEXT)
        self.assertTrue('2' not in wf._commits)

        self.assertFalse('2' in wf._edges)
        self.assertFalse('2' in wf._back_edges)
        pass

    def collapse_at_dead_end_test(self):
        wf = Workflow('', '')
        wf.add_commit(self._commit1)
        wf.add_commit(self._commit2)
        wf.add_commit(self._commit3) # Dead end which cannot be collapsed

        self.assertEqual(len(wf._commits), 3)
        wf.collapse_repro_commits()
        self.assertEqual(len(wf._commits), 2)

        self.assertEqual(wf._commits[self._commit1.hash].text, self._commit1._comment + '\n' + self._commit1.hash)
        self.assertEqual(wf._commits[self._commit3.hash].text, Commit.COLLAPSED_TEXT)
        self.assertTrue('2' not in wf._commits)
        pass

    def collapse_metric_commit_test(self):
        value = 0.812345
        branches = ['master', 'try_smth']
        metric_commit3 = Commit('2', '1', 'name1', 'today', 'DVC repro-run ...',
                                True, value, branch_tips=branches)

        wf = Workflow('', '')
        wf.add_commit(self._commit1)
        wf.add_commit(metric_commit3)
        wf.add_commit(self._commit3)

        self.assertEqual(len(wf._commits), 3)
        wf.collapse_repro_commits()
        self.assertEqual(len(wf._commits), 2)

        self.assertEqual(wf._commits['3']._target_metric, value)
        self.assertEqual(wf._commits['3'].branch_tips, branches)
        pass