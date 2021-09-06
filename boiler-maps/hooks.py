from pathlib import Path
from typing import Any

from dotenv import load_dotenv


def load_environmental_variables(product: Any):
    load_dotenv()


def create_folder_structure(product: Any) -> None:
    dirpath = Path("data")
    dirpath.mkdir(exist_ok=True)
    (dirpath / "external").mkdir(exist_ok=True)
    (dirpath / "processed").mkdir(exist_ok=True)
    (dirpath / "html").mkdir(exist_ok=True)
    (dirpath / "notebooks").mkdir(exist_ok=True)
