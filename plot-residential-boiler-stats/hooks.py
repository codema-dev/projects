from pathlib import Path

from dotenv import load_dotenv


def load_environmental_variables_and_create_folder_structure() -> None:
    load_dotenv()
    dirpath = Path("data")
    dirpath.mkdir(exist_ok=True)
    (dirpath / "external").mkdir(exist_ok=True)
    (dirpath / "processed").mkdir(exist_ok=True)
    (dirpath / "html").mkdir(exist_ok=True)
    (dirpath / "notebooks").mkdir(exist_ok=True)
