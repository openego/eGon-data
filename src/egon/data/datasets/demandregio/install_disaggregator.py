"""This module downloads and installs demandregio's disaggregator from GitHub

"""
import os
import shutil
from pathlib import Path

import egon.data.config
from egon.data import subprocess


def clone_and_install():
    """ Clone and install repository of demandregio's disaggregator

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

    # Delete repository if it already exists
    if repo_path.exists() and repo_path.is_dir():
        shutil.rmtree(repo_path)

    # Create subfolder
    os.mkdir(repo_path)

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
