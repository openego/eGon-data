# -*- coding: utf-8 -*-
"""
The central module aggregating ch4 store caverns (stores) and ch4 generators 
"""
import pandas as pd
import requests

from egon.data import db
from egon.data.datasets import Dataset


class GasAggregation(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="GasAggregation",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(aggregate_gas),
        )


def aggregate_gas(scn_name="eGon2035"):
    """Aggregation of ch4 stores and ch4 generators with same properties at the same bus.

    Parameters
    ----------
    scn_name : str
        Name of the scenario

    Returns
    -------

    """
    # Connect to local database
    engine = db.engine()

    components = [
        # Generator
        {
            "columns": "scn_name, generator_id, bus, p_nom, carrier",
            "table": "egon_etrago_generator",
            "strategies": {
                "scn_name": "first",
                "generator_id": "first",
                "p_nom": "sum",
                "bus": "first",
                "carrier": "first"
            },
        },
        # Store
        {
            "columns": "scn_name, store_id, bus, e_nom, carrier",
            "table": "egon_etrago_store",
            "strategies": {
                "scn_name": "first",
                "store_id": "first",
                "e_nom": "sum",
                "bus": "first",
                "carrier": "first"
            },
        },
    ]

    for comp in components:
        df = db.select_dataframe(
            f"""SELECT {comp["columns"]}
                    FROM grid.{comp["table"]}
                    WHERE scn_name = '{scn_name}' 
                    AND carrier = 'CH4';"""
        )
        print(df)
        df = df.groupby(["bus", "carrier"]).agg(comp["strategies"])
        print(df)

        # Clean table
        db.execute_sql(
            f"""DELETE FROM grid.{comp["table"]} 
                        WHERE carrier = 'CH4'
                        AND scn_name = '{scn_name}';"""
        )

        # Insert data to db
        df.to_sql(
            comp["table"],
            engine,
            schema="grid",
            index=False,
            if_exists="append",
        )
