import pytest


@pytest.mark.requires(minversion=(2, 28, 0))
def test_exp_show(make_project, monkeypatch, bench_dvc, dvc_bin):
    url = "https://github.com/iterative/example-get-started"
    rev = "main"
    path = make_project(url, rev=rev)
    monkeypatch.chdir(path)

    dvc_bin("exp", "pull", "-A", "--no-cache", "origin")

    bench_dvc("exp", "show", "-A", "--no-pager")
