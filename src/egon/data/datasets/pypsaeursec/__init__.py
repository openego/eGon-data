"""The central module containing all code dealing with importing data from
the pysa-eur-sec scenario parameter creation
"""

from pathlib import Path
from urllib.request import urlretrieve
import os
import tarfile

import importlib_resources as resources
import pandas as pd
import pypsa
import yaml

from egon.data import __path__
from egon.data.datasets import Dataset
from egon.data.datasets.scenario_parameters import get_sector_parameters
import egon.data.config
import egon.data.subprocess as subproc


class PypsaEurSec(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="PypsaEurSec",
            version="0.0.9",
            dependencies=dependencies,
            tasks=(run_pypsa_eur_sec),
        )


def run_pypsa_eur_sec():

    # Skip execution of pypsa-eur-sec by default until optional task is implemented
    execute_pypsa_eur_sec = False

    if execute_pypsa_eur_sec:

        cwd = Path(".")
        filepath = cwd / "run-pypsa-eur-sec"
        filepath.mkdir(parents=True, exist_ok=True)

        pypsa_eur_repos = filepath / "pypsa-eur"
        pypsa_eur_repos_data = pypsa_eur_repos / "data"
        technology_data_repos = filepath / "technology-data"
        pypsa_eur_sec_repos = filepath / "pypsa-eur-sec"
        pypsa_eur_sec_repos_data = pypsa_eur_sec_repos / "data"

        if not pypsa_eur_repos.exists():
            subproc.run(
                [
                    "git",
                    "clone",
                    "--branch",
                    "v0.4.0",
                    "https://github.com/PyPSA/pypsa-eur.git",
                    pypsa_eur_repos,
                ]
            )

            # subproc.run(
            #     ["git", "checkout", "4e44822514755cdd0289687556547100fba6218b"],
            #     cwd=pypsa_eur_repos,
            # )

            file_to_copy = os.path.join(
                __path__[0], "datasets", "pypsaeursec", "pypsaeur", "Snakefile"
            )

            subproc.run(["cp", file_to_copy, pypsa_eur_repos])

            # Read YAML file
            path_to_env = pypsa_eur_repos / "envs" / "environment.yaml"
            with open(path_to_env, "r") as stream:
                env = yaml.safe_load(stream)

            env["dependencies"].append("gurobi")

            # Write YAML file
            with open(path_to_env, "w", encoding="utf8") as outfile:
                yaml.dump(
                    env, outfile, default_flow_style=False, allow_unicode=True
                )

            datafile = "pypsa-eur-data-bundle.tar.xz"
            datapath = pypsa_eur_repos / datafile
            if not datapath.exists():
                urlretrieve(
                    f"https://zenodo.org/record/3517935/files/{datafile}", datapath
                )
                tar = tarfile.open(datapath)
                tar.extractall(pypsa_eur_repos_data)

        if not technology_data_repos.exists():
            subproc.run(
                [
                    "git",
                    "clone",
                    "--branch",
                    "v0.3.0",
                    "https://github.com/PyPSA/technology-data.git",
                    technology_data_repos,
                ]
            )

        if not pypsa_eur_sec_repos.exists():
            subproc.run(
                [
                    "git",
                    "clone",
                    "https://github.com/openego/pypsa-eur-sec.git",
                    pypsa_eur_sec_repos,
                ]
            )

        datafile = "pypsa-eur-sec-data-bundle.tar.gz"
        datapath = pypsa_eur_sec_repos_data / datafile
        if not datapath.exists():
            urlretrieve(
                f"https://zenodo.org/record/5824485/files/{datafile}", datapath
            )
            tar = tarfile.open(datapath)
            tar.extractall(pypsa_eur_sec_repos_data)

        with open(filepath / "Snakefile", "w") as snakefile:
            snakefile.write(
                resources.read_text("egon.data.datasets.pypsaeursec", "Snakefile")
            )

        subproc.run(
            [
                "snakemake",
                "-j1",
                "--directory",
                filepath,
                "--snakefile",
                filepath / "Snakefile",
                "--use-conda",
                "--conda-frontend=conda",
                "Main",
            ]
        )
    else:
        print('Execution of pypsa_eur_sec was not required.')





