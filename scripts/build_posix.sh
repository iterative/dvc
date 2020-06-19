#!/bin/bash

set -e
set -x

if [[ "$(uname)" == 'Linux' ]]; then
  INSTALL_DIR=usr
  FPM_FLAGS=
else
  INSTALL_DIR=usr/local
  FPM_FLAGS='--osxpkg-identifier-prefix com.iterative'
  FPM_FLAGS+=' --after-install scripts/fpm/after-install.sh'
  FPM_FLAGS+=' --after-remove scripts/fpm/after-remove.sh'
fi

BUILD_DIR=build
BIN_DIR=$BUILD_DIR/$INSTALL_DIR/bin
DESC='Data Version Control - datasets, models, and experiments versioning for ML or data science projects'
LIB_DIR=$BUILD_DIR/$INSTALL_DIR/lib

FPM_PACKAGE_DIRS="usr"
ZSH_CMPLT_DIR=usr/share/zsh/site-functions/_dvc
if [[ "$(uname)" == 'Linux' ]]; then
  BASH_CMPLT_DIR=etc/bash_completion.d
  FPM_PACKAGE_DIRS="$FPM_PACKAGE_DIRS etc"
else
  BASH_CMPLT_DIR=usr/local/etc/bash_completion.d
fi

print_error() {
  echo -e "\e[31m$1\e[0m" >&2
}

if [ ! -d "dvc" ]; then
  print_error "Please run this script from repository root"
  exit 1
fi

trap 'print_error "FAIL"; exit 1' ERR

print_info() {
  echo -e "\e[32m$1\e[0m"
}

command_exists() {
  command -v $1 >/dev/null 2>&1
}

fpm_build() {
  print_info "Building $1..."
  VERSION=$(python -c "import dvc; from dvc import __version__; print(str(__version__))")
  fpm -s dir \
    -f \
    -t $1 \
    --description "$DESC" \
    $FPM_FLAGS \
    -n dvc \
    -v $VERSION \
    -C $BUILD_DIR \
    $FPM_PACKAGE_DIRS
}

cleanup() {
  print_info "Cleaning up..."
  rm -rf build
}

install_dependencies() {
  print_info "Installing fpm..."
  if command_exists dnf; then
    sudo dnf install ruby-devel gcc make rpm-build
  elif command_exists yum; then
    sudo yum install ruby-devel gcc make rpm-build
  elif command_exists apt-get; then
    sudo apt-get update -y
    sudo apt-get install ruby-dev build-essential rpm python-pip python-dev
  elif command_exists brew; then
    brew install ruby
  else
    echo "Unable to install fpm dependencies" && exit 1
  fi

  gem install --no-document fpm

  # https://github.com/iterative/dvc/issues/2970
  gem uninstall -i /Users/travis/.rvm/gems/ruby-2.4.3@global rubygems-bundler

  print_info "Installing requirements..."
  pip install .[all]
  pip install -r scripts/build-requirements.txt
}

build_dvc() {
  print_info "Building dvc binary..."
  pyinstaller \
    --additional-hooks-dir $(pwd)/scripts/hooks dvc/__main__.py \
    --name dvc \
    --distpath $LIB_DIR \
    --specpath $BUILD_DIR

  $LIB_DIR/dvc/dvc --help

  # NOTE: in osxpkg fpm replaces symlinks with actual file that it
  # points to, so we need to use after-install hook. See FPM_FLAGS
  # above.
  if [[ "$(uname)" == 'Linux' ]]; then
    mkdir -p $BIN_DIR
    pushd $BIN_DIR
    ln -s ../lib/dvc/dvc dvc
    popd
    $BIN_DIR/dvc --help
  fi

  # NOTE: temporarily not adding scripts to mac package. See [1]
  # [1] https://github.com/iterative/dvc/issues/2585
  if [[ "$(uname)" == 'Linux' ]]; then
    mkdir -p $BUILD_DIR/$BASH_CMPLT_DIR
    $LIB_DIR/dvc/dvc completion -s bash > $BUILD_DIR/$BASH_CMPLT_DIR/dvc

    mkdir -p $BUILD_DIR/$ZSH_CMPLT_DIR
    $LIB_DIR/dvc/dvc completion -s zsh > $BUILD_DIR/$ZSH_CMPLT_DIR/_dvc
  fi
}

build() {
  cleanup
  echo "PKG = \"$1\"" >dvc/utils/build.py
  build_dvc
  fpm_build $1
}

install_dependencies

if [[ "$(uname)" == 'Linux' ]]; then
  build rpm
  build deb
else
  build osxpkg
fi

cleanup

print_info "Successfully built dvc packages"
