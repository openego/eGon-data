"""The central module containing all code dealing with importing data from
the pysa-eur-sec scenario parameter creation
"""

from pathlib import Path
from urllib.request import urlretrieve
import tarfile
import os

import importlib_resources as resources
import pandas as pd

from egon.data import db
from egon.data.importing.nep_input_data import scenario_config
from egon.data import __path__
import egon.data.subprocess as subproc
import yaml



def run_pypsa_eur_sec():

    cwd = Path(".")
    filepath = cwd / "run-pypsa-eur-sec"
    filepath.mkdir(parents=True, exist_ok=True)

    pypsa_eur_repos = filepath / "pypsa-eur"
    technology_data_repos = filepath / "technology-data"
    pypsa_eur_sec_repos = filepath / "pypsa-eur-sec"
    pypsa_eur_sec_repos_data = pypsa_eur_sec_repos / "data/"

    if not pypsa_eur_repos.exists():
        subproc.run(
            [
                "git",
                "clone",
                "--branch",
                "master",
                "https://github.com/PyPSA/pypsa-eur.git",
                pypsa_eur_repos,
            ],
        )

        subproc.run(
            [
                "git",
                "checkout",
                "4e44822514755cdd0289687556547100fba6218b",
            ],
            cwd=pypsa_eur_repos,
        )
        
        file_to_copy = os.path.join(
        __path__[0] + "/importing" + "/pypsaeursec/pypsaeur/Snakefile")
        
        subproc.run(
            [
                "cp",
                file_to_copy,
                pypsa_eur_repos,
            ],
        )
        
        # Read YAML file
        path_to_env = pypsa_eur_repos / "envs/environment.yaml"
        with open(path_to_env, 'r') as stream:
            env = yaml.safe_load(stream)
            
        env["dependencies"].append("gurobi")

        # Write YAML file
        with open(path_to_env, 'w', encoding='utf8') as outfile:
            yaml.dump(env, outfile, default_flow_style=False, allow_unicode=True)

    if not technology_data_repos.exists():
        subproc.run(
            [
                "git",
                "clone",
                "--branch",
                "v0.2.0",
                "https://github.com/PyPSA/technology-data.git",
                technology_data_repos,
            ],
        )

    if not pypsa_eur_sec_repos.exists():
        subproc.run(
            [
                "git",
                "clone",
                "https://github.com/openego/pypsa-eur-sec.git",
                pypsa_eur_sec_repos,
            ],
        )

    datafile = "pypsa-eur-sec-data-bundle-201012.tar.gz"
    datapath = pypsa_eur_sec_repos_data / datafile
    if not datapath.exists():
        urlretrieve(f"https://nworbmot.org/{datafile}", datapath)
        tar = tarfile.open(datapath)
        tar.extractall(pypsa_eur_sec_repos_data)

    with open(filepath / "Snakefile", "w") as snakefile:
        snakefile.write(
            resources.read_text("egon.data.importing.pypsaeursec", "Snakefile")
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
        ],
    )


def pypsa_eur_sec_eGon100_capacities():
    """Inserts installed capacities for the eGon100 scenario

    Returns
    -------
    None.

    """

    # Connect to local database
    engine = db.engine()

    # Delete rows if already exist
    db.execute_sql(
        "DELETE FROM supply.egon_scenario_capacities "
        "WHERE scenario_name = 'eGon100'"
    )

    # read-in installed capacities
    target_file = (
        resources.files("egon.data.importing.pypsaeursec")
        / scenario_config("eGon100")["paths"]["capacities"]
    )

    df = pd.read_csv(target_file, skiprows=5)
    df.columns = ["component", "country", "carrier", "capacity"]
    df["scenario_name"] = "eGon100"
    
    desired_countries = ['DE', 'AT', 'CH', 'CZ', 'PL', 'SE', 'NO', 'DK',
                         'GB', 'NL', 'BE', 'FR', 'LU']
    
    new_df =pd.DataFrame(columns=(df.columns))
    for i in desired_countries:
        new_df_1 = df[df.country.str.startswith(i, na=False)]
        new_df = new_df.append(new_df_1)


    # Insert data to db
    new_df.to_sql(
        "egon_scenario_capacities",
        engine,
        schema="supply",
        if_exists="append",
        index=new_df.index,
    )
