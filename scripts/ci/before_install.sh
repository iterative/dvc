#!/bin/bash

set -x
set -e

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    ulimit -a
    sudo sysctl -w kern.maxproc=2048
    sudo sysctl -w kern.maxprocperuid=2048
    echo '\nulimit -u 2048' >> ~/.bash_profile
    ulimit -a
    brew update
    brew upgrade pyenv
    # NOTE: used to install and run our style checking tools
    osx_python_ver=3.6.2
    eval "$(pyenv init -)"
    pyenv install --skip-existing --keep --verbose $osx_python_ver &> pyenv.log || tail -n 50 pyenv.log
    pyenv shell $osx_python_ver
    python --version
elif [[ "$TRAVIS_OS_NAME" == "windows" ]]; then
    choco install python
    # NOTE: used to install and run our style checking tools
    PATH=/c/Python37:/c/Python37/Scripts:$PATH
fi

pip install .[tests]

# stop the build if there are any readme formatting errors
python setup.py checkdocs

# stop the build if there are any black or flake8 errors
black ./ --check
flake8 ./

if [[ "$TRAVIS_OS_NAME" != "windows" ]]; then
    #NOTE: ssh keys for ssh test to be able to ssh to the localhost
    ssh-keygen -t rsa -N "" -f mykey
    mkdir -p ~/.ssh
    cp mykey ~/.ssh/id_rsa
    cp mykey.pub ~/.ssh/id_rsa.pub
    cat mykey.pub >> ~/.ssh/authorized_keys
    ssh-keyscan localhost >> ~/.ssh/known_hosts
    ssh localhost ls &> /dev/null
    ssh-keyscan 127.0.0.1 >> ~/.ssh/known_hosts
    ssh 127.0.0.1 ls &> /dev/null
    ssh-keyscan 0.0.0.0 >> ~/.ssh/known_hosts
    ssh 0.0.0.0 ls &> /dev/null
fi

scriptdir="$(dirname $0)"
echo > env.sh
echo 'set -e' >> env.sh
echo 'set -x' >> env.sh

if [ "$TRAVIS_OS_NAME" == "linux" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ]
then
  bash "$scriptdir/install_azurite.sh"
  bash "$scriptdir/install_oss.sh"
  bash "$scriptdir/install_hadoop.sh"
fi

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    brew install openssl
    brew cask install google-cloud-sdk
fi

if [[ -n "$PYTHON_VER" ]]; then
    if [[ "$TRAVIS_OS_NAME" == "windows" ]]; then
        if [[ "$PYTHON_VER" == "2.7" ]]; then
            choco install python2
            echo 'PATH="/c/Python27:/c/Python27/Scripts:$PATH"' >> env.sh
        else
            echo 'PATH="/c/Python37:/c/Python37/Scripts:$PATH"' >> env.sh
        fi
    else
        eval "$(pyenv init -)"
        echo 'eval "$(pyenv init -)"' >> env.sh
        pyenv install --skip-existing "$PYTHON_VER"
        echo "pyenv global $PYTHON_VER" >> env.sh
    fi
fi
