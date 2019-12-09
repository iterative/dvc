FROM debian:buster
MAINTAINER Anton Abrosimov <anton@abrosimov.online>

ENV CONTAINER docker
ENV EXPORT_DIR /mnt/packages

# APT configuration.
RUN echo "debconf debconf/frontend select text" | debconf-set-selections && \
    echo "debconf debconf/frontend select noninteractive" | debconf-set-selections && \
    apt-get update && \
    apt-get install -y --no-install-recommends apt-utils && \
    apt-get autoremove -y --purge && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Upgrade base system.
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get autoremove -y --purge && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Make export dir.
RUN mkdir -p $EXPORT_DIR

# Install build deps.
RUN apt-get update && \
    apt-get -y --no-install-recommends install \
        git \
        python3-setuptools \
        python3-wheel \
        python3-pip \
        python3-stdeb \
        python3-all \
        python-all \
        python3-pytest \
        python3-pytest-runner \
        build-essential \
        dh-python \
        fakeroot \
        && \
    apt-get autoremove -y --purge && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Build treelib

# Build nanotime

# Build funcy
## Required for tests.
RUN pip3 install \
        whatever

# Build grandalf
RUN apt-get update && \
    apt-get -y --no-install-recommends install \
        python3-pyparsing \
        python3-future \
        && \
    apt-get autoremove -y --purge && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Build jsonpath-ng
RUN apt-get update && \
    apt-get -y --no-install-recommends install \
        python3-ply \
        python3-six \
        python3-decorator \
        && \
    apt-get autoremove -y --purge && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

## Required for tests.
RUN apt-get update && \
    apt-get -y --no-install-recommends install \
        python3-dev \
        python3-testscenarios \
        && \
    apt-get autoremove -y --purge && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    pip3 install \
        oslotest \
        && \
    apt-get remove -y --purge python3-dev && \
    apt-get autoremove -y --purge

# Build configparser
## Required for building.
RUN pip3 install \
        setuptools_scm

# Build pydrive
RUN apt-get update && \
    apt-get -y --no-install-recommends install \
        python3-yaml \
        python3-oauth2client \
        # After merge of https://github.com/gsuitedevs/PyDrive/pull/180 into pydrive's master,
        # usage of google-api-python-client can be removed from DVC.
        python3-googleapi \
        && \
    apt-get autoremove -y --purge && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Build azure-storage-blob.
RUN apt-get update && \
    apt-get -y --no-install-recommends install \
        python3-cryptography \
        python3-msrest \
        python3-requests \
        python3-six \
        && \
    apt-get autoremove -y --purge && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Build aliyun.
RUN apt-get update && \
    apt-get -y --no-install-recommends install \
        python3-pycryptodome \
        python3-crcmod \
        python3-requests \
        python3-jmespath \
        python3-crypto \
        && \
    apt-get autoremove -y --purge && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Build DVC.
RUN apt-get update && \
    apt-get -y --no-install-recommends install \
        python3-ply \
        python3-future \
        python3-colorama \
        python3-configobj \
        python3-setuptools \
        python3-requests \
        python3-distro \
        python3-appdirs \
        python3-inflect \
        python3-humanize \
        python3-pathspec \
        python3-shortuuid \
        python3-tqdm \
        python3-packaging \
        python3-yaml \
        python3-ruamel.yaml \
        python3-git \
        python3-pyasn1 \
        python3-voluptuous \
        python3-networkx \
        python3-flufl.lock \
        python3-boto3 \
        && \
    apt-get autoremove -y --purge && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# PostProc.
COPY src/build.sh /root/build.sh
RUN chmod 0755 /root/build.sh

ENV HOME /root
WORKDIR /root

USER root

ENTRYPOINT ["/bin/sh", "-c"]
CMD ["bash"]
