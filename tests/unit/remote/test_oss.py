from dvc.tree.oss import OSSTree

bucket_name = "bucket-name"
endpoint = "endpoint"
key_id = "Fq2UVErCz4I6tq"
key_secret = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"


def test_init(dvc):
    prefix = "some/prefix"
    url = f"oss://{bucket_name}/{prefix}"
    config = {
        "url": url,
        "oss_key_id": key_id,
        "oss_key_secret": key_secret,
        "oss_endpoint": endpoint,
    }
    tree = OSSTree(dvc, config)
    assert tree.path_info == url
    assert tree.endpoint == endpoint
    assert tree.key_id == key_id
    assert tree.key_secret == key_secret
