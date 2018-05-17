#!/bin/bash

set -e
set -x

BUILD_DIR=build
BIN_DIR=$BUILD_DIR/$INSTALL_DIR/bin

if [[ "$(uname)" == 'Linux' ]]; then
	INSTALL_DIR=usr
	FPM_FLAGS=
else
	INSTALL_DIR=usr/local
	FPM_FLAGS='--osxpkg-identifier-prefix com.dataversioncontrol'
fi

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
	VERSION=$(python -c "import dvc; from dvc import VERSION; print(str(VERSION))")
	fpm -s dir -f -t $1 $FPM_FLAGS -n dvc -v $VERSION -C $BUILD_DIR $INSTALL_DIR
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

	gem install --no-ri --no-rdoc fpm

	print_info "Installing requirements..."
	pip install -r requirements.txt

	print_info "Installing pyinstaller..."
	pip install pyinstaller
}

build_dvc()
{
	print_info "Building dvc binary..."
	pyinstaller --onefile --additional-hooks-dir $(pwd)/scripts/hooks dvc/__main__.py --name dvc --distpath $BIN_DIR --specpath $BUILD_DIR

	$BIN_DIR/dvc --help
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
