import os
from unittest.mock import ANY

import pytest

from dvc.fs import download

lfs_prefetch_params = [
    pytest.param("abc", "abc", id="plain"),
    pytest.param(
        "*",
        "[*]",
        marks=pytest.mark.skipif(
            os.name == "nt",
            reason="forbidden character `*` on Windows filesystem",
        ),
        id="escape-*",
    ),
    pytest.param(
        "**",
        "[*][*]",
        marks=pytest.mark.skipif(
            os.name == "nt", reason="forbidden character `*` on Windows filesystem"
        ),
        id="escape-**",
    ),
    pytest.param(
        "?",
        "[?]",
        marks=pytest.mark.skipif(
            os.name == "nt", reason="forbidden character `?` on Windows filesystem"
        ),
        id="escape-?",
    ),
    pytest.param("[abc]", "[[]abc]", id="escape-[seq]"),
    pytest.param("[!abc]", "[[]!abc]", id="escape-[!seq]"),
]


@pytest.mark.parametrize("dirname, include_name", lfs_prefetch_params)
def test_lfs_prefetch_directory(tmp_dir, dvc, scm, mocker, dirname, include_name):
    mock_fetch = mocker.patch("scmrepo.git.lfs.fetch")
    tmp_dir.scm_gen(
        {
            ".gitattributes": "data/**/* filter=lfs diff=lfs merge=lfs -text",
            f"data/{dirname}/test.txt": "test data",
        },
        commit="init lfs",
    )
    rev = scm.get_rev()
    with dvc.switch(rev):
        download(dvc.dvcfs, f"data/{dirname}", "data")
        mock_fetch.assert_called_once_with(
            scm, [rev], include=[f"/data/{include_name}/**"], progress=ANY
        )


@pytest.mark.parametrize("basename, include_name", lfs_prefetch_params)
def test_lfs_prefetch_file(tmp_dir, dvc, scm, mocker, basename, include_name):
    mock_fetch = mocker.patch("scmrepo.git.lfs.fetch")
    tmp_dir.scm_gen(
        {
            ".gitattributes": "data/**/* filter=lfs diff=lfs merge=lfs -text",
            f"data/{basename}.txt": "test data",
        },
        commit="init lfs",
    )
    rev = scm.get_rev()
    with dvc.switch(rev):
        download(dvc.dvcfs, f"data/{basename}.txt", "data")
        mock_fetch.assert_called_once_with(
            scm, [rev], include=[f"/data/{include_name}.txt"], progress=ANY
        )
