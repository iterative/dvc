import os
import uuid
from contextlib import contextmanager

import pytest
from funcy import cached_property

from dvc.remote.base import Remote
from dvc.remote.gs import GSRemoteTree
from dvc.utils import env2bool

from .base import Base

TEST_GCP_REPO_BUCKET = os.environ.get("DVC_TEST_GCP_REPO_BUCKET", "dvc-test")

TEST_GCP_CREDS_FILE = os.path.abspath(
    os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS",
        os.path.join("scripts", "ci", "gcp-creds.json"),
    )
)
# Ensure that absolute path is used
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = TEST_GCP_CREDS_FILE


class GCP(Base):
    @staticmethod
    def should_test():
        from subprocess import CalledProcessError, check_output

        do_test = env2bool("DVC_TEST_GCP", undefined=None)
        if do_test is not None:
            return do_test

        if not os.path.exists(TEST_GCP_CREDS_FILE):
            return False

        try:
            check_output(
                [
                    "gcloud",
                    "auth",
                    "activate-service-account",
                    "--key-file",
                    TEST_GCP_CREDS_FILE,
                ]
            )
        except (CalledProcessError, OSError):
            return False
        return True

    @cached_property
    def config(self):
        return {
            "url": self.url,
            "credentialpath": TEST_GCP_CREDS_FILE,
        }

    @staticmethod
    def _get_storagepath():
        return TEST_GCP_REPO_BUCKET + "/" + str(uuid.uuid4())

    @staticmethod
    def get_url():
        return "gs://" + GCP._get_storagepath()

    @classmethod
    @contextmanager
    def remote(cls, repo):
        yield Remote(GSRemoteTree(repo, {"url": cls.get_url()}))

    @staticmethod
    def put_objects(remote, objects):
        client = remote.tree.gs
        bucket = client.get_bucket(remote.path_info.bucket)
        for key, body in objects.items():
            bucket.blob((remote.path_info / key).path).upload_from_string(body)


@pytest.fixture
def gs():
    if not GCP.should_test():
        pytest.skip("no gs")
    yield GCP()


@pytest.fixture
def gs_remote(tmp_dir, dvc, gs):
    tmp_dir.add_remote(config=gs.config)
    yield gs
