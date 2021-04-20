"""The central module containing all code dealing with importing data from
the pysa-eur-sec scenario parameter creation
"""

import os
import pandas as pd
from egon.data import db
from egon.data.importing.nep_input_data import scenario_config
from sqlalchemy.ext.declarative import declarative_base
import egon.data.subprocess as subproc


### will be later imported from another file ###
Base = declarative_base()


def run_pypsa_eur_sec():

    # execute pypsa-eur-sec
    import os
    import egon.data.subprocess as subproc
    from pathlib import Path

    filepath = Path(".")
    pypsa_eur_repos = filepath / "pypsa-eur"
    technology_data_repos = filepath / "technology-data"
    pypsa_eur_sec_repos = filepath / "pypsa-eur-sec"
    pypsa_eur_sec_repos_data = pypsa_eur_sec_repos / "data/"

    if not os.path.exists(pypsa_eur_repos):
        subproc.run(
            ["git", "clone", "https://github.com/PyPSA/pypsa-eur.git"],
            cwd=filepath,
        )

    if not os.path.exists(technology_data_repos):
        subproc.run(
            ["git", "clone", "https://github.com/PyPSA/technology-data.git"],
            cwd=filepath,
        )

    if not os.path.exists(pypsa_eur_sec_repos):
        subproc.run(
            ["git", "clone", "https://github.com/openego/pypsa-eur-sec.git"],
            cwd=filepath,
        )

    subproc.run(
        [
            "wget",
            "https://nworbmot.org/pypsa-eur-sec-data-bundle-201012.tar.gz",
        ],
        cwd=pypsa_eur_sec_repos_data,
    )

    subproc.run(
        ["tar", "xvzf", "pypsa-eur-sec-data-bundle-201012.tar.gz"],
        cwd=pypsa_eur_sec_repos_data,
    )

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
    target_file = os.path.join(
        os.path.dirname(__file__),
        scenario_config("eGon100")["paths"]["capacities"],
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
