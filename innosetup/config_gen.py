# This script generates config.ini for setup.iss script
from dvc.main import VERSION

try:
    import configparser as ConfigParser
except ImportError:
    import ConfigParser

config = ConfigParser.ConfigParser()
config.add_section('Version')
config.set('Version', 'Version', VERSION)

with open('config.ini', 'w') as f:
    config.write(f)
