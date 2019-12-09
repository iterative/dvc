================
DVC building env
================

Required:
  * Docker
  * Docker-compose

Check versions in the `src/build.sh`

Append new entry and version number in the `debian/changelog`

Build env:
  .. code:: sh

    docker-compose build

Build packages:
  .. code:: sh

    docker-compose run --rm dvc

Packages could be found in ``./packages`` after building.

Enjoy!

*Don't forget to rebuild env after updating `building.sh`*
