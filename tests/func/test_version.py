import re

from dvc.main import main


def test_run_in_repo(dvc_repo):
    assert main(["version"]) == 0


def test_run_outside_of_repo(repo_dir):
    assert main(["version"]) == 0


def test_info_in_repo(dvc_repo, caplog):
    main(["version"])

    assert re.search(re.compile(r"DVC version: \d+\.\d+\.\d+"), caplog.text)
    assert re.search(re.compile(r"Python version: \d\.\d\.\d"), caplog.text)
    assert re.search(re.compile(r"Platform: .*"), caplog.text)
    assert re.search(
        re.compile(r"Filesystem type \(cache directory\): .*"), caplog.text
    )
    assert re.search(
        re.compile(r"Filesystem type \(root directory\): .*"), caplog.text
    )
    assert re.search(
        re.compile(r"(Cache: (.*link - (True|False)(,\s)?){3})"), caplog.text
    )


def test_info_outside_of_repo(repo_dir, caplog):
    main(["version"])

    assert re.search(re.compile(r"DVC version: \d+\.\d+\.\d+"), caplog.text)
    assert re.search(re.compile(r"Python version: \d\.\d\.\d"), caplog.text)
    assert re.search(re.compile(r"Platform: .*"), caplog.text)
    assert re.search(
        re.compile(r"Filesystem type \(root directory\): .*"), caplog.text
    )
    assert not re.search(
        re.compile(r"Filesystem type \(cache directory\): .*"), caplog.text
    )
    assert not re.search(
        re.compile(r"(Cache: (.*link - (True|False)(,\s)?){3})"), caplog.text
    )
