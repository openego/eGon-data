"""This module downloads and installs demandregio's disaggregator from GitHub

"""
import os
import egon.data.config
from egon.data import  subprocess
from pathlib import Path


def clone_and_install():
    """ Clone and install repository of demandregio's disaggregator

    Returns
    -------
    None.

    """

    source = egon.data.config.datasets()['demandregio_installation']['sources']

    repo_path = Path(".") / (egon.data.config.datasets()
                   ['demandregio_installation']['targets']["path"])

    if not os.path.exists(repo_path):
        os.mkdir(repo_path)
        subprocess.run(
            [
                "git",
                "clone",
                "--single-branch",
                "--branch",
                source["branch"],
                source["git-repository"],
            ],
            cwd=repo_path
        )

        subprocess.run(
            [
                "pip",
                "install",
                "-e",
                (repo_path/'disaggregator').absolute(),
            ],
        )