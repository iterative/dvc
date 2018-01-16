import os
import stat
import shutil
import filecmp

from dvc.main import main
from tests.basic_env import TestDvc
from tests.test_repro import TestRepro


class TestCheckout(TestRepro):
    def setUp(self):
        super(TestCheckout, self).setUp()

        self.orig = 'orig'
        shutil.copy(self.FOO, self.orig)
        os.chmod(self.FOO, stat.S_IWRITE)
        os.unlink(self.FOO)

    def test(self):
        self.dvc.checkout()
        self._test_checkout()

    def _test_checkout(self):
        self.assertTrue(os.path.isfile(self.FOO))
        self.assertTrue(filecmp.cmp(self.FOO, self.orig))


class TestCmdCheckout(TestCheckout):
    def test(self):
        ret = main(['checkout'])
        self.assertEqual(ret, 0)
        self._test_checkout()


class CheckoutBase(TestDvc):
    GIT_IGNORE = '.gitignore'

    def commit_data_file(self, fname, content='random text'):
        with open(fname, 'w') as fd:
            fd.write(content)
        self.dvc.add(fname)
        self.dvc.scm.add([fname + '.dvc', '.gitignore'])
        self.dvc.scm.commit('adding ' + fname)

    def read_ignored(self):
        return list(map(lambda s: s.strip('\n'), open(self.GIT_IGNORE).readlines()))


class TestRemoveFilesWhenCheckout(CheckoutBase):
    def test(self):
        fname = 'file_in_a_branch'
        branch_master = 'master'
        branch_1 = 'b1'

        self.dvc.scm.add(self.dvc.scm.untracked_files())
        self.dvc.scm.commit('add all files')

        # add the file into a separate branch
        self.dvc.scm.checkout(branch_1, True)
        main(['checkout'])
        self.commit_data_file(fname)

        # Checkout back in master
        self.dvc.scm.checkout(branch_master)
        self.assertTrue(os.path.exists(fname))

        # Make sure `dvc checkout` removes the file
        # self.dvc.checkout()
        main(['checkout'])
        self.assertFalse(os.path.exists(fname))


class TestGitIgnoreBasic(CheckoutBase):
    def test(self):
        fname1 = 'file_1'
        fname2 = 'file_2'
        fname3 = 'file_3'

        self.dvc.scm.add(self.dvc.scm.untracked_files())
        self.dvc.scm.commit('add all files')

        self.assertFalse(os.path.exists(self.GIT_IGNORE))

        self.commit_data_file(fname1)
        self.commit_data_file(fname2)
        self.dvc.run(cmd='python {} {} {}'.format(self.CODE, self.FOO, fname3),
                     deps=[self.CODE, self.FOO],
                     outs_no_cache=[fname3])

        self.assertTrue(os.path.exists(self.GIT_IGNORE))

        ignored = self.read_ignored()

        self.assertEqual(len(ignored), 2)

        self.assertIn(fname1, ignored)
        self.assertIn(fname2, ignored)


class TestGitIgnoreWhenCheckout(CheckoutBase):
    def test(self):
        fname_master = 'file_in_a_master'
        branch_master = 'master'
        fname_branch = 'file_in_a_branch'
        branch_1 = 'b1'

        self.dvc.scm.add(self.dvc.scm.untracked_files())
        self.dvc.scm.commit('add all files')
        self.commit_data_file(fname_master)

        self.dvc.scm.checkout(branch_1, True)
        main(['checkout'])
        self.commit_data_file(fname_branch)

        self.dvc.scm.checkout(branch_master)
        main(['checkout'])

        ignored = self.read_ignored()

        self.assertEqual(len(ignored), 1)
        self.assertIn(fname_master, ignored)

        self.dvc.scm.checkout(branch_1)
        main(['checkout'])
        ignored = self.read_ignored()
        self.assertIn(fname_branch, ignored)
