./unittests.sh
./functests.sh

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
	./scripts/build_macos.sh
else
	./scripts/build_linux.sh
fi

./scripts/build_package.sh
