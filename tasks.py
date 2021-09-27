from pathlib import Path

from invoke import task


@task
def merge_environment_ymls(c):
    cwd = Path(__name__).parent
    project_paths = [
        p for p in cwd.iterdir() if (p.is_dir()) and (not p.name[0] == ".")
    ]
    environment_yml_paths = " ".join(
        [
            str(p / "environment.yml")
            for p in project_paths
            if (p / "environment.yml").exists()
        ]
    )
    c.run(
        "conda-merge "
        + environment_yml_paths
        + " " + str(cwd / "environment.meta.yml")
        + " > environment.yml"
    )
