# This script generates config.ini for setup.iss script
from dvc import VERSION
from dvc.utils.compat import ConfigParser, open

config = ConfigParser.ConfigParser()
config.add_section("Version")
config.set("Version", "Version", VERSION)

with open("scripts/innosetup/config.ini", "w") as f:
    config.write(f)
