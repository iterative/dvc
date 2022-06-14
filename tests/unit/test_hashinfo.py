from dvc_data.hashfile.hash_info import HashInfo


def test_as_raw():
    hash_info = HashInfo(
        "md5", "a1d0c6e83f027327d8461063f4ac58a6.dir", "objname"
    )

    raw = hash_info.as_raw()

    assert hash_info.name == "md5"
    assert hash_info.value == "a1d0c6e83f027327d8461063f4ac58a6.dir"
    assert hash_info.obj_name == "objname"

    assert raw.name == "md5"
    assert raw.value == "a1d0c6e83f027327d8461063f4ac58a6"
    assert raw.obj_name == "objname"
