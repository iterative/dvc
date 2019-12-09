#!/bin/bash
# Building packages.

# Exit when error
set -e

# Variables and versions.
EXPORT_DIR=/mnt/packages
TREELIB_VERSION=tags/v1.3.2
FUNCY_VERSION=tags/1.14
GRANDALF_VERSION=tags/v0.6
JSONPATHNG_VERSION=origin/master  # Test error in the last tagged version.
NANOTIME_VERSION=origin/master  # No tags in repo.
CONFIGPARSER_VERSION=tags/v4.0.2
PYDRIVE_VERSION=origin/master
AZURE_STORAGE_VERSION=tags/azure-storage-blob_12.1.0
ALIYUN_SDK_VERSION=origin/master  # No tags in repo.
ALIYUN_OSS_VERSION=tags/v2.6.1
DVC_VERSION=origin/master  # Set version tag.


# Builders.
function build_treelib {
    git clone "https://github.com/caesar0301/treelib.git"
    cd treelib
    git checkout $TREELIB_VERSION
    python3 setup.py --command-packages=stdeb.command bdist_deb
    mv deb_dist/python3-treelib*.deb $EXPORT_DIR
    cd ../
}

function build_funcy {
    git clone "https://github.com/Suor/funcy.git"
    cd funcy
    git checkout $FUNCY_VERSION
    python3 setup.py --command-packages=stdeb.command bdist_deb
    mv deb_dist/python3-funcy*.deb $EXPORT_DIR
    cd ../
}

function build_grandalf {
    git clone "https://github.com/bdcht/grandalf.git"
    cd grandalf
    git checkout $GRANDALF_VERSION
    python3 setup.py --command-packages=stdeb.command bdist_deb
    mv deb_dist/python3-grandalf*.deb $EXPORT_DIR
    cd ../
}

function build_jsonpathng {
    git clone "https://github.com/h2non/jsonpath-ng.git"
    cd jsonpath-ng
    git checkout $JSONPATHNG_VERSION
    python3 setup.py --command-packages=stdeb.command bdist_deb
    mv deb_dist/python3-jsonpath-ng*.deb $EXPORT_DIR && \
    cd ../
}

function build_nanotime {
    git clone "https://github.com/jbenet/nanotime.git"
    cd nanotime/python
    git checkout $NANOTIME_VERSION
    python3 setup.py --command-packages=stdeb.command bdist_deb
    mv deb_dist/python3-nanotime*.deb $EXPORT_DIR
    cd ../../
}

function build_configparser {
    git clone "https://github.com/jaraco/configparser.git"
    cd configparser/
    git checkout $CONFIGPARSER_VERSION
    python3 setup.py --command-packages=stdeb.command bdist_deb
    mv deb_dist/python3-configparser*.deb $EXPORT_DIR && \
    cd ../
}

function build_pydrive {
    git clone "https://github.com/gsuitedevs/PyDrive.git"
    cd PyDrive/
    git checkout $PYDRIVE_VERSION
    # Unit tests SKIPED! Requires manual intervention.
    DEB_BUILD_OPTIONS=nocheck python3 setup.py --command-packages=stdeb.command bdist_deb
    mv deb_dist/python3-pydrive*.deb $EXPORT_DIR && \
    cd ../
}

function build_azure_storage {
    git clone "https://github.com/Azure/azure-sdk-for-python.git"
    cd azure-sdk-for-python/sdk/core/azure-core/
    git checkout $AZURE_STORAGE_VERSION
    python3 setup.py --command-packages=stdeb.command bdist_deb
    dpkg -i deb_dist/python3-azure-core*.deb
    mv deb_dist/python3-azure-core*.deb $EXPORT_DIR
    cd ../../

    cd storage/azure-storage-blob/
    python3 setup.py --command-packages=stdeb.command bdist_deb
    mv deb_dist/python3-azure-storage-blob*.deb $EXPORT_DIR
    cd ../../../../
}

function build_aliyun {
    git clone "https://github.com/aliyun/aliyun-openapi-python-sdk.git"
    cd aliyun-openapi-python-sdk/aliyun-python-sdk-core/
    git checkout $ALIYUN_SDK_VERSION
    mv ./setup3.py ./setup.py
    # Unit tests SKIPED! Requires manual intervention.
    DEB_BUILD_OPTIONS=nocheck python3 setup.py --command-packages=stdeb.command bdist_deb
    dpkg -i deb_dist/python3-*.deb
    mv deb_dist/python3-*.deb $EXPORT_DIR && \
    cd ../
    cd aliyun-python-sdk-kms/
    python3 setup.py --command-packages=stdeb.command bdist_deb
    dpkg -i deb_dist/python3-*.deb
    mv deb_dist/python3-*.deb $EXPORT_DIR && \
    cd ../../

    git clone "https://github.com/aliyun/aliyun-oss-python-sdk.git"
    cd aliyun-oss-python-sdk/
    git checkout $ALIYUN_OSS_VERSION
    python3 setup.py --command-packages=stdeb.command bdist_deb
    mv deb_dist/python3-*.deb $EXPORT_DIR && \
    cd ../
}

function build_google-cloud-storage {
    git clone "https://github.com/googleapis/google-cloud-python.git"
    cd google-cloud-python/storage/
    git checkout $GOOGLECLOUD_STORAGE_VERSION
    python3 setup.py --command-packages=stdeb.command bdist_deb
    mv deb_dist/python3-configparser*.deb $EXPORT_DIR && \
    cd ../../
}

function build_dvc {
    dpkg -i $EXPORT_DIR/*.deb
    git clone "https://github.com/iterative/dvc.git"
    cd dvc/
    git checkout $DVC_VERSION
    dpkg-buildpackage -us -uc -rfakeroot
    cd ../
    mv dvc*.deb $EXPORT_DIR
    dpkg -i $EXPORT_DIR/*.deb
}

# Execution.
build_treelib
build_funcy
build_grandalf
build_jsonpathng
build_nanotime
build_configparser
build_pydrive
build_azure_storage
build_aliyun
build_dvc
exit 0
