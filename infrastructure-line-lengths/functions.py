from pathlib import Path
import urllib


def check_file_exists(filepath):
    assert filepath.exists(), f"Please download & unzip {filepath} to data/external/"
