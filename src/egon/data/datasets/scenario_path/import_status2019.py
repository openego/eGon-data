"""
Read eTraGo tables for the status2019 and import it to db
"""

import os
import subprocess

import pandas as pd

from egon.data import config, db
from egon.data.datasets import Dataset


class import_status2019(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="import_status2019",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(import_scn_status2019,),
        )


def import_scn_status2019():
    """
    Read and import the scenario status2019 and import it into db

    Parameters
    ----------
    *No parameters required

    """
    # Connect to the data base
    con = db.engine()

    # Clean existing data for status2019
    tables = pd.read_sql(
        """
        SELECT tablename FROM pg_catalog.pg_tables
        WHERE schemaname = 'grid'
        """,
        con,
    )

    tables = tables[
        ~tables["tablename"].isin(
            [
                "egon_etrago_carrier",
                "egon_etrago_temp_resolution",
            ]
        )
    ]

    for table in tables["tablename"]:
        db.execute_sql(
            f"""
        DELETE FROM grid.{table} WHERE scn_name = 'status2019';
        """
        )

    my_env = os.environ.copy()
    my_env["PGPASSWORD"] = "data"

    config_data = config.settings()["egon-data"]
    database = config_data["--database-name"]
    host = config_data["--database-host"]
    port = config_data["--database-port"]
    user = config_data["--database-user"]

    for table in tables["tablename"]:
        subprocess.Popen(
            [
                "pg_restore",
                "-d",
                database,
                "--host",
                host,
                "--port",
                port,
                "-U",
                user,
                "-a",
                "--single-transaction",
                f"--table={table}",
                "data_bundle_powerd_data/PoWerD_status2019-v2.backup",
            ],
            env=my_env,
        )