# pylint:disable=abstract-method

import os
import uuid

import pytest

from dvc.path_info import CloudURLInfo
from dvc.utils import env2bool

from .base import Base


class Azure(Base, CloudURLInfo):
    @staticmethod
    def should_test():
        do_test = env2bool("DVC_TEST_AZURE", undefined=None)
        if do_test is not None:
            return do_test

        return os.getenv("AZURE_STORAGE_CONTAINER_NAME") and os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING"
        )

    @staticmethod
    def get_url():
        container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
        assert container_name is not None
        return "azure://{}/{}".format(container_name, str(uuid.uuid4()))


@pytest.fixture
def azure():
    if not Azure.should_test():
        pytest.skip("no azure running")
    yield Azure(Azure.get_url())


@pytest.fixture
def azure_remote(tmp_dir, dvc, azure):
    tmp_dir.add_remote(config=azure.config)
    yield azure
