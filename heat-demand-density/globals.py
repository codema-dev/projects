from configparser import ConfigParser
from pathlib import Path


def get_basedir() -> Path:
    return Path(__name__).parent


BASE_DIR = get_basedir()
CONFIG = ConfigParser()
CONFIG.read(BASE_DIR / "config.ini")
DATA_DIR = BASE_DIR / "data"
