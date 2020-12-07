import os

import pytest

from dvc.scm.base import SCMError
from tests.basic_env import TestDvcGit


# Behaves the same as SCM but will test against all suported Git backends.
# tmp_dir.scm will still contain a default SCM instance.
@pytest.fixture(params=["gitpython", "dulwich"])
def git(tmp_dir, scm, request):
    from dvc.scm.git import Git

    git_ = Git(os.fspath(tmp_dir), backends=[request.param])
    yield git_
    git_.close()


class TestGit(TestDvcGit):
    def test_belongs_to_scm_true_on_gitignore(self):
        path = os.path.join("path", "to", ".gitignore")
        self.assertTrue(self.dvc.scm.belongs_to_scm(path))

    def test_belongs_to_scm_true_on_git_internal(self):
        path = os.path.join("path", "to", ".git", "internal", "file")
        self.assertTrue(self.dvc.scm.belongs_to_scm(path))

    def test_belongs_to_scm_false(self):
        path = os.path.join("some", "non-.git", "file")
        self.assertFalse(self.dvc.scm.belongs_to_scm(path))


def test_walk_with_submodules(tmp_dir, scm, git_dir):
    git_dir.scm_gen(
        {"foo": "foo", "bar": "bar", "dir": {"data": "data"}},
        commit="add dir and files",
    )
    scm.gitpython.repo.create_submodule(
        "submodule", "submodule", url=os.fspath(git_dir)
    )
    scm.commit("added submodule")

    files = []
    dirs = []
    tree = scm.get_tree("HEAD")
    for _, dnames, fnames in tree.walk("."):
        dirs.extend(dnames)
        files.extend(fnames)

    # currently we don't walk through submodules
    assert not dirs
    assert set(files) == {".gitmodules", "submodule"}


def test_walk_onerror(tmp_dir, scm):
    def onerror(exc):
        raise exc

    tmp_dir.scm_gen(
        {"foo": "foo"}, commit="init",
    )
    tree = scm.get_tree("HEAD")

    # path does not exist
    for _ in tree.walk("dir"):
        pass
    with pytest.raises(OSError):
        for _ in tree.walk("dir", onerror=onerror):
            pass

    # path is not a directory
    for _ in tree.walk("foo"):
        pass
    with pytest.raises(OSError):
        for _ in tree.walk("foo", onerror=onerror):
            pass


def test_is_tracked(tmp_dir, scm):
    tmp_dir.scm_gen(
        {
            "tracked": "tracked",
            "dir": {"data": "data", "subdir": {"subdata": "subdata"}},
        },
        commit="add dirs and files",
    )
    tmp_dir.gen({"untracked": "untracked", "dir": {"untracked": "untracked"}})

    # sanity check
    assert (tmp_dir / "untracked").exists()
    assert (tmp_dir / "tracked").exists()
    assert (tmp_dir / "dir" / "untracked").exists()
    assert (tmp_dir / "dir" / "data").exists()
    assert (tmp_dir / "dir" / "subdir" / "subdata").exists()

    assert not scm.is_tracked("untracked")
    assert not scm.is_tracked(os.path.join("dir", "untracked"))

    assert scm.is_tracked("tracked")
    assert scm.is_tracked("dir")
    assert scm.is_tracked(os.path.join("dir", "data"))
    assert scm.is_tracked(os.path.join("dir", "subdir"))
    assert scm.is_tracked(os.path.join("dir", "subdir", "subdata"))


def test_is_tracked_unicode(tmp_dir, scm):
    tmp_dir.scm_gen("ṭṝḁḉḵḗḋ", "tracked", commit="add unicode")
    tmp_dir.gen("ṳṋṭṝḁḉḵḗḋ", "untracked")
    assert scm.is_tracked("ṭṝḁḉḵḗḋ")
    assert not scm.is_tracked("ṳṋṭṝḁḉḵḗḋ")


def test_no_commits(tmp_dir):
    from dvc.scm.git import Git
    from tests.dir_helpers import git_init

    git_init(".")
    assert Git().no_commits

    tmp_dir.gen("foo", "foo")
    Git().add(["foo"])
    Git().commit("foo")

    assert not Git().no_commits


def test_branch_revs(tmp_dir, scm):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = scm.get_rev()

    expected = []
    for i in range(1, 5):
        tmp_dir.scm_gen({"file": f"{i}"}, commit=f"{i}")
        expected.append(scm.get_rev())

    for rev in scm.branch_revs("master", init_rev):
        assert rev == expected.pop()
    assert len(expected) == 0


def test_set_ref(tmp_dir, git):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = tmp_dir.scm.get_rev()
    tmp_dir.scm_gen({"file": "1"}, commit="commit")
    commit_rev = tmp_dir.scm.get_rev()

    git.set_ref("refs/foo/bar", init_rev)
    assert (
        init_rev
        == (tmp_dir / ".git" / "refs" / "foo" / "bar").read_text().strip()
    )

    with pytest.raises(SCMError):
        git.set_ref("refs/foo/bar", commit_rev, old_ref=commit_rev)
    git.set_ref("refs/foo/bar", commit_rev, old_ref=init_rev)
    assert (
        commit_rev
        == (tmp_dir / ".git" / "refs" / "foo" / "bar").read_text().strip()
    )

    git.set_ref("refs/foo/baz", "refs/heads/master", symbolic=True)
    assert (
        "ref: refs/heads/master"
        == (tmp_dir / ".git" / "refs" / "foo" / "baz").read_text().strip()
    )


def test_get_ref(tmp_dir, git):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = tmp_dir.scm.get_rev()
    tmp_dir.gen(
        {
            os.path.join(".git", "refs", "foo", "bar"): init_rev,
            os.path.join(
                ".git", "refs", "foo", "baz"
            ): "ref: refs/heads/master",
        }
    )

    assert init_rev == git.get_ref("refs/foo/bar")
    assert init_rev == git.get_ref("refs/foo/baz")
    assert "refs/heads/master" == git.get_ref("refs/foo/baz", follow=False)
    assert git.get_ref("refs/foo/qux") is None


def test_remove_ref(tmp_dir, git):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = tmp_dir.scm.get_rev()
    tmp_dir.gen(os.path.join(".git", "refs", "foo", "bar"), init_rev)
    tmp_dir.scm_gen({"file": "1"}, commit="commit")
    commit_rev = tmp_dir.scm.get_rev()

    with pytest.raises(SCMError):
        git.remove_ref("refs/foo/bar", old_ref=commit_rev)
    git.remove_ref("refs/foo/bar", old_ref=init_rev)
    assert not (tmp_dir / ".git" / "refs" / "foo" / "bar").exists()


def test_refs_containing(tmp_dir, scm):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = scm.get_rev()
    tmp_dir.gen(
        {
            os.path.join(".git", "refs", "foo", "bar"): init_rev,
            os.path.join(".git", "refs", "foo", "baz"): init_rev,
        }
    )

    expected = {"refs/foo/bar", "refs/foo/baz", "refs/heads/master"}
    assert expected == set(scm.get_refs_containing(init_rev))


@pytest.mark.parametrize("use_url", [True, False])
def test_push_refspec(tmp_dir, scm, make_tmp_dir, use_url):
    tmp_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = scm.get_rev()
    tmp_dir.gen(
        {
            os.path.join(".git", "refs", "foo", "bar"): init_rev,
            os.path.join(".git", "refs", "foo", "baz"): init_rev,
        }
    )
    remote_dir = make_tmp_dir("git-remote", scm=True)
    url = "file://{}".format(remote_dir.resolve().as_posix())
    scm.gitpython.repo.create_remote("origin", url)

    with pytest.raises(SCMError):
        scm.push_refspec("bad-remote", "refs/foo/bar", "refs/foo/bar")

    remote = url if use_url else "origin"
    scm.push_refspec(remote, "refs/foo/bar", "refs/foo/bar")
    assert init_rev == remote_dir.scm.get_ref("refs/foo/bar")

    remote_dir.scm.checkout("refs/foo/bar")
    assert init_rev == remote_dir.scm.get_rev()
    assert "0" == (remote_dir / "file").read_text()

    scm.push_refspec(remote, "refs/foo/", "refs/foo/")
    assert init_rev == remote_dir.scm.get_ref("refs/foo/baz")

    scm.push_refspec(remote, None, "refs/foo/baz")
    assert remote_dir.scm.get_ref("refs/foo/baz") is None


def test_fetch_refspecs(tmp_dir, scm, make_tmp_dir):
    remote_dir = make_tmp_dir("git-remote", scm=True)
    url = "file://{}".format(remote_dir.resolve().as_posix())

    remote_dir.scm_gen({"file": "0"}, commit="init")
    init_rev = remote_dir.scm.get_rev()
    remote_dir.gen(
        {
            os.path.join(".git", "refs", "foo", "bar"): init_rev,
            os.path.join(".git", "refs", "foo", "baz"): init_rev,
        }
    )

    scm.fetch_refspecs(
        url, ["refs/foo/bar:refs/foo/bar", "refs/foo/baz:refs/foo/baz"]
    )
    assert init_rev == scm.get_ref("refs/foo/bar")
    assert init_rev == scm.get_ref("refs/foo/baz")

    remote_dir.scm.checkout("refs/foo/bar")
    assert init_rev == remote_dir.scm.get_rev()
    assert "0" == (remote_dir / "file").read_text()
