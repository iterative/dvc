# This script generates config.ini for setup.iss script
import configparser

from dvc import __version__

config = configparser.ConfigParser()
config.add_section("Version")
config.set("Version", "Version", __version__)

with open("scripts/innosetup/config.ini", "w") as f:
    config.write(f)
