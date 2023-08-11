from dvc.utils.fs import remove


def test_repro_allow_missing(tmp_dir, dvc):
    tmp_dir.gen("fixed", "fixed")
    dvc.stage.add(name="create-foo", cmd="echo foo > foo", deps=["fixed"], outs=["foo"])
    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    (create_foo, copy_foo) = dvc.reproduce()

    remove("foo")
    remove(create_foo.outs[0].cache_path)
    remove(dvc.stage_cache.cache_dir)

    ret = dvc.reproduce(allow_missing=True)
    # both stages are skipped
    assert not ret


def test_repro_allow_missing_dry_with_dvc_2_file(tmp_dir, dvc):
    """https://github.com/iterative/dvc/issues/9818"""

    # A .dvc file in 2.X format
    (tmp_dir / "data.dvc").dump(
        {
            "outs": [
                {
                    "md5": "14d187e749ee5614e105741c719fa185.dir",
                    "size": 18999874,
                    "nfiles": 183,
                    "path": "data",
                }
            ]
        }
    )

    (tmp_dir / "dvc.lock").dump(
        {
            "schema": "2.0",
            "stages": {
                "cat-data-foo": {
                    "cmd": "cat data/foo > foo",
                    "deps": [
                        {
                            # The file as dependency in 3.X format
                            "path": "data",
                            "hash": "md5",
                            "md5": "14d187e749ee5614e105741c719fa185.dir",
                            "size": 18999874,
                            "nfiles": 183,
                        }
                    ],
                    "outs": [
                        {
                            "path": "foo",
                            "hash": "md5",
                            "md5": "069f9d4b9c418b935f7bec4a094ba535",
                            "size": 30,
                        }
                    ],
                }
            },
        }
    )
    dvc.stage.add(
        name="cat-data-foo", cmd="cat data/foo > foo", deps=["data"], outs=["foo"]
    )

    ret = dvc.reproduce(allow_missing=True, dry=True)
    assert not ret


def test_repro_allow_missing_and_pull(tmp_dir, dvc, mocker, local_remote):
    tmp_dir.gen("fixed", "fixed")
    dvc.stage.add(name="create-foo", cmd="echo foo > foo", deps=["fixed"], outs=["foo"])
    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    (create_foo,) = dvc.reproduce("create-foo")

    dvc.push()

    remove("foo")
    remove(create_foo.outs[0].cache_path)
    remove(dvc.stage_cache.cache_dir)

    ret = dvc.reproduce(pull=True, allow_missing=True)
    # create-foo is skipped ; copy-foo pulls missing dep
    assert len(ret) == 1


def test_repro_allow_missing_upstream_stage_modified(
    tmp_dir, dvc, mocker, local_remote
):
    """https://github.com/iterative/dvc/issues/9530"""
    tmp_dir.gen("params.yaml", "param: 1")
    dvc.stage.add(
        name="create-foo", cmd="echo ${param} > foo", params=["param"], outs=["foo"]
    )
    dvc.stage.add(name="copy-foo", cmd="cp foo bar", deps=["foo"], outs=["bar"])
    dvc.reproduce()

    dvc.push()

    tmp_dir.gen("params.yaml", "param: 2")
    (create_foo,) = dvc.reproduce("create-foo")
    dvc.push()
    remove("foo")
    remove(create_foo.outs[0].cache_path)

    ret = dvc.reproduce(pull=True, allow_missing=True)
    # create-foo is skipped ; copy-foo pulls modified dep
    assert len(ret) == 1
