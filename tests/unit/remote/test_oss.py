from dvc_oss import OSSFileSystem

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
    fs = OSSFileSystem(**config)
    assert fs.fs_args["endpoint"] == endpoint
    assert fs.fs_args["key"] == key_id
    assert fs.fs_args["secret"] == key_secret
