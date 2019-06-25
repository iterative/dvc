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

print_error()
{
	echo -e "\e[31m$1\e[0m"
}

if [ ! -d "dvc" ]; then
	print_error "Please run this script from repository root"
	exit 1
fi

trap 'print_error "FAIL"; exit 1' ERR

print_info()
{
	echo -e "\e[32m$1\e[0m"
}

command_exists()
{
	command -v $1 > /dev/null 2>&1
}

fpm_build()
{
	print_info "Building $1..."
	VERSION=$(python -c "import dvc; from dvc import __version__; print(str(__version__))")
	fpm -s dir \
	    -f \
	    -t $1 \
            --description "$DESC" \
	    $FPM_FLAGS \
	    -n dvc \
	    -v $VERSION \
	    -C $BUILD_DIR usr
}

cleanup()
{
	print_info "Cleaning up..."
	rm -rf build
}

install_dependencies()
{
	print_info "Installing fpm..."
	if command_exists dnf; then
		sudo dnf install ruby-devel gcc make rpm-build
	elif command_exists yum; then
		sudo yum install ruby-devel gcc make rpm-build
	elif command_exists apt-get; then
		sudo apt-get update -y
		sudo apt-get install ruby-dev build-essential rpm python-pip python-dev
	elif command_exists brew; then
		brew update
		brew install ruby
	else
		echo "Unable to install fpm dependencies" && exit 1
	fi

	gem install --no-document fpm

	print_info "Installing requirements..."
	pip install -e .[all]

	print_info "Installing pyinstaller..."
	pip install pyinstaller
}

build_dvc()
{
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
}

cleanup
install_dependencies
build_dvc

if [[ "$(uname)" == 'Linux' ]]; then
	fpm_build rpm
	fpm_build deb
else
	fpm_build osxpkg
fi

cleanup

print_info "Successfully built dvc packages"
