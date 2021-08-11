from pathlib import Path
import urllib


def download_file(url: str, filepath: str) -> None:
    if not filepath.exists():
        urllib.request.urlretrieve(url=url, filename=str(filepath))
