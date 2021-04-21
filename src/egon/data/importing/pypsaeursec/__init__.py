"""The central module containing all code dealing with importing data from
the pysa-eur-sec scenario parameter creation
"""

import importlib_resources as resources
import pandas as pd

from egon.data import db
from egon.data.importing.nep_input_data import scenario_config


def run_pypsa_eur_sec():

    from pathlib import Path
    from urllib.request import urlretrieve
    import tarfile

    import egon.data.subprocess as subproc

    filepath = Path(".")
    pypsa_eur_repos = filepath / "pypsa-eur"
    technology_data_repos = filepath / "technology-data"
    pypsa_eur_sec_repos = filepath / "pypsa-eur-sec"
    pypsa_eur_sec_repos_data = pypsa_eur_sec_repos / "data/"

    if not pypsa_eur_repos.exists():
        subproc.run(
            ["git", "clone", "https://github.com/PyPSA/pypsa-eur.git"],
        )

    if not technology_data_repos.exists():
        subproc.run(
            ["git", "clone", "https://github.com/PyPSA/technology-data.git"],
        )

    if not pypsa_eur_sec_repos.exists():
        subproc.run(
            ["git", "clone", "https://github.com/openego/pypsa-eur-sec.git"],
        )

    datafile = "pypsa-eur-sec-data-bundle-201012.tar.gz"
    datapath = pypsa_eur_sec_repos_data / datafile
    if not datapath.exists():
        urlretrieve(f"https://nworbmot.org/{datafile}", datapath)
        tar = tarfile.open(datapath)
        tar.extractall(pypsa_eur_sec_repos_data)

    subproc.run(
        ["snakemake", "-j1", "prepare_sector_networks"],
        cwd=pypsa_eur_sec_repos,
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

    # Insert data to db
    df.to_sql(
        "egon_scenario_capacities",
        engine,
        schema="supply",
        if_exists="append",
        index=df.index,
    )
