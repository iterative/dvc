import os
import git
import filecmp

from tests.utils import trees_equal


def test_install_and_uninstall(repo_dir, dvc_repo, pkg):
    name = os.path.basename(pkg.root_dir)
    pkg_dir = os.path.join(repo_dir.root_dir, ".dvc", "pkg")
    mypkg_dir = os.path.join(pkg_dir, name)

    dvc_repo.pkg.install(pkg.root_dir)
    assert os.path.exists(pkg_dir)
    assert os.path.isdir(pkg_dir)
    assert os.path.exists(mypkg_dir)
    assert os.path.isdir(mypkg_dir)
    assert os.path.isdir(os.path.join(mypkg_dir, ".git"))

    dvc_repo.pkg.install(pkg.root_dir)
    assert os.path.exists(pkg_dir)
    assert os.path.isdir(pkg_dir)
    assert os.path.exists(mypkg_dir)
    assert os.path.isdir(mypkg_dir)
    assert os.path.isdir(os.path.join(mypkg_dir, ".git"))

    git_repo = git.Repo(mypkg_dir)
    assert git_repo.active_branch.name == "master"

    dvc_repo.pkg.uninstall(name)
    assert not os.path.exists(mypkg_dir)

    dvc_repo.pkg.uninstall(name)
    assert not os.path.exists(mypkg_dir)


def test_uninstall_corrupted(repo_dir, dvc_repo):
    name = os.path.basename("mypkg")
    pkg_dir = os.path.join(repo_dir.root_dir, ".dvc", "pkg")
    mypkg_dir = os.path.join(pkg_dir, name)

    os.makedirs(mypkg_dir)

    dvc_repo.pkg.uninstall(name)
    assert not os.path.exists(mypkg_dir)


def test_install_version(repo_dir, dvc_repo, pkg):
    name = os.path.basename(pkg.root_dir)
    pkg_dir = os.path.join(repo_dir.root_dir, ".dvc", "pkg")
    mypkg_dir = os.path.join(pkg_dir, name)

    dvc_repo.pkg.install(pkg.root_dir, version="branch")
    assert os.path.exists(pkg_dir)
    assert os.path.isdir(pkg_dir)
    assert os.path.exists(mypkg_dir)
    assert os.path.isdir(mypkg_dir)
    assert os.path.isdir(os.path.join(mypkg_dir, ".git"))

    git_repo = git.Repo(mypkg_dir)
    assert git_repo.active_branch.name == "branch"


def test_import(repo_dir, dvc_repo, pkg):
    name = os.path.basename(pkg.root_dir)

    src = pkg.FOO
    dst = pkg.FOO + "_imported"

    dvc_repo.pkg.install(pkg.root_dir)
    dvc_repo.pkg.imp(name, src, dst)

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(repo_dir.FOO, dst, shallow=False)


def test_import_dir(repo_dir, dvc_repo, pkg):
    name = os.path.basename(pkg.root_dir)

    src = pkg.DATA_DIR
    dst = pkg.DATA_DIR + "_imported"

    dvc_repo.pkg.install(pkg.root_dir)
    dvc_repo.pkg.imp(name, src, dst)

    assert os.path.exists(dst)
    assert os.path.isdir(dst)
    trees_equal(src, dst)


def test_import_url(repo_dir, dvc_repo, pkg):
    name = os.path.basename(pkg.root_dir)
    pkg_dir = os.path.join(repo_dir.root_dir, ".dvc", "pkg")
    mypkg_dir = os.path.join(pkg_dir, name)

    src = pkg.FOO
    dst = pkg.FOO + "_imported"

    dvc_repo.pkg.imp(pkg.root_dir, src, dst)

    assert os.path.exists(pkg_dir)
    assert os.path.isdir(pkg_dir)
    assert os.path.exists(mypkg_dir)
    assert os.path.isdir(mypkg_dir)
    assert os.path.isdir(os.path.join(mypkg_dir, ".git"))

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(repo_dir.FOO, dst, shallow=False)


def test_import_url_version(repo_dir, dvc_repo, pkg):
    name = os.path.basename(pkg.root_dir)
    pkg_dir = os.path.join(repo_dir.root_dir, ".dvc", "pkg")
    mypkg_dir = os.path.join(pkg_dir, name)

    src = "version"
    dst = src

    dvc_repo.pkg.imp(pkg.root_dir, src, dst, version="branch")

    assert os.path.exists(pkg_dir)
    assert os.path.isdir(pkg_dir)
    assert os.path.exists(mypkg_dir)
    assert os.path.isdir(mypkg_dir)
    assert os.path.isdir(os.path.join(mypkg_dir, ".git"))

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    with open(dst, "r+") as fobj:
        assert fobj.read() == "branch"
