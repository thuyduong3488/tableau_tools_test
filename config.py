"""
Set the working environment.
Environments: "development", "production"
"""

from util.iniparser import parse_ini
import os

__ENV__ = "development" \
          ""
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
__CONFIG_FILE_PATH__ = os.path.join(ROOT_DIR, 'config.ini')
CONFIG = parse_ini(__CONFIG_FILE_PATH__)["CONFIG"]
print("Working in {} environment\n".format(__ENV__.upper()))
def get_env():
    return __ENV__
