set -e
set -x

if [[ "$TRAVIS_OS_NAME" == "windows" ]]; then
    where python
    where pip
else
    which python
    which pip
fi

if [[ -z "$PYTHON_VER" ]]; then
    exit 0
fi

if [[ "$TRAVIS_OS_NAME" == "windows" ]]; then
    if [[ "$(python -c 'import sys; print(sys.version[:3])')" != "$PYTHON_VER" ]]; then
        exit 1
    fi
else
    if [[ "$(python --version 2>&1)" != "Python $PYTHON_VER" ]]; then
        exit 1
    fi
fi
