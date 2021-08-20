from configparser import ConfigParser
from pathlib import Path


def get_basedir() -> Path:
    return Path(__name__).parent


HERE = get_basedir()
DATA_DIR = HERE / "data"
