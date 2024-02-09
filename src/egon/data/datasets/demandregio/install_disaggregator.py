"""This module downloads and installs demandregio's disaggregator from GitHub

"""
from pathlib import Path
from subprocess import check_output
import importlib.util
import os
import shutil

from egon.data import logger, subprocess
import egon.data.config


def clone_and_install():
    """Clone and install repository of demandregio's disaggregator

    Returns
    -------
    None.

    """

    source = egon.data.config.datasets()["demandregio_installation"]["sources"]

    repo_path = Path(".") / (
        egon.data.config.datasets()["demandregio_installation"]["targets"][
            "path"
        ]
    )

    try:
        status = check_output(
            ["git", "status"], cwd=(repo_path / "disaggregator").absolute()
        )
        if status.startswith(b"Auf Branch features/pandas-update"):
            logger.info("Demandregio cloned and right branch checked out.")
        else:
            raise ImportError(
                "Demandregio cloned but wrong branch checked "
                "out. Please checkout features/pandas-update"
            )
        spec = importlib.util.find_spec("disaggregator")
        if spec is not None:
            logger.info("Demandregio is not installed. Installing now.")
            # Install disaggregator from path
            subprocess.run(
                [
                    "pip",
                    "install",
                    "-e",
                    (repo_path / "disaggregator").absolute(),
                ]
            )

    except subprocess.CalledProcessError and FileNotFoundError:
        # Create subfolder
        os.makedirs(repo_path, exist_ok=True)

        # Clone from GitHub repository
        subprocess.run(
            [
                "git",
                "clone",
                "--single-branch",
                "--branch",
                source["branch"],
                source["git-repository"],
            ],
            cwd=repo_path,
        )

        # Install disaggregator from path
        subprocess.run(
            ["pip", "install", "-e", (repo_path / "disaggregator").absolute()]
        )
