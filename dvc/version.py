# Used from setup.py, so don't pull any additional dependencies
import os
import subprocess


def generate_version(base_version):
    """Generate a version with information about the git repository"""
    pkg_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

    if not is_git_repo(pkg_dir) or not have_git(pkg_dir):
        return base_version

    return "{base_version}+{short_sha}{dirty}".format(
        base_version=base_version,
        short_sha=git_revision(pkg_dir).decode("utf-8")[0:6],
        dirty=".mod" if is_dirty(pkg_dir) else "",
    )


def is_git_repo(dir_path):
    """Is the given directory version-controlled with git?"""
    return os.path.exists(os.path.join(dir_path, ".git"))


def have_git(dir_path):
    """Can we run the git executable?"""
    try:
        subprocess.check_output(["git", "--help"])
        return True
    except subprocess.CalledProcessError:
        return False
    except OSError:
        return False


def git_revision(dir_path):
    """Get the SHA-1 of the HEAD of a git repository."""
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=dir_path
    ).strip()


def is_dirty(dir_path):
    """Check whether a git repository has uncommitted changes."""
    try:
        subprocess.check_call(["git", "diff", "--quiet"], cwd=dir_path)
        return False
    except subprocess.CalledProcessError:
        return True


__version__ = generate_version(base_version="0.35.0")
