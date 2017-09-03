from unittest import TestCase

from dvc.workflow import Workflow, Commit


class TestWorkflow(TestCase):
    def setUp(self):
        self._commit4 = Commit('4', '3', 'name1', 'today', 'comment4', False)
        self._commit3 = Commit('3', '2', 'name1', 'today', 'DVC repro-run ...', False)
        self._commit2 = Commit('2', '1', 'name1', 'today', 'DVC repro-run ...', False)
        self._commit1 = Commit('1', '', 'name1', 'today', 'comment1', False)

    def test_commits(self):
        self.assertFalse(self._commit1.is_repro)
        self.assertTrue(self._commit2.is_repro)
        self.assertTrue(self._commit3.is_repro)
        self.assertFalse(self._commit4.is_repro)
        pass

    def test_workflow_basic(self):
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

    def test_collapse(self):
        wf = Workflow('', '')
        wf.add_commit(self._commit1)
        wf.add_commit(self._commit2)
        wf.add_commit(self._commit3)
        wf.add_commit(self._commit4)

        wf.collaps_repro_commits()

        self.assertEqual(len(wf._commits), 3)
        self.assertEqual(wf._commits[self._commit1.hash].text, self._commit1._comment + '\n' + self._commit1.hash)
        self.assertEqual(wf._commits[self._commit3.hash].text, Commit.COLLAPSED_TEXT)
        self.assertTrue('2' not in wf._commits)

        self.assertFalse('2' in wf._edges)
        self.assertFalse('2' in wf._back_edges)
        pass

    def test_collapse_at_dead_end(self):
        wf = Workflow('', '')
        wf.add_commit(self._commit1)
        wf.add_commit(self._commit2)
        wf.add_commit(self._commit3) # Dead end which cannot be collapsed

        self.assertEqual(len(wf._commits), 3)
        wf.collaps_repro_commits()
        self.assertEqual(len(wf._commits), 2)

        self.assertEqual(wf._commits[self._commit1.hash].text, self._commit1._comment + '\n' + self._commit1.hash)
        self.assertEqual(wf._commits[self._commit3.hash].text, Commit.COLLAPSED_TEXT)
        self.assertTrue('2' not in wf._commits)
        pass
