"""The central module containing code to merge data on electricity demand
and feed this data into the corresponding etraGo tables.

"""


import egon.data.config
import geopandas as gpd
import numpy as np
import pandas as pd
from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.industry.temporal import (
    insert_osm_ind_load,
    insert_sites_ind_load,
)
from sqlalchemy import Column, String, Float, Integer, ARRAY
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


def merge_electricity_demand():
    """Sum all electricity demand curves up per bus
    Returns
    -------
    None.
    """

    # Read information from configuration file
    sources = egon.data.config.datasets()["etrago_electricity"][
        "sources"
    ]

    scenario = 'eGon2035'

    # Select data on CTS electricity demands per bus
    cts_curves = db.select_dataframe(
            f"""SELECT subst_id, p_set FROM
                {sources['cts_curves']['schema']}.
                {sources['cts_curves']['table']}
                WHERE scn_name = '{scenario}'""",
            index_col='subst_id',
        )

    # Rename index
    cts_curves.index.rename("bus", inplace=True)


    # Select data on industrial demands assigned to osm landuse areas

    ind_curves_osm = db.select_dataframe(
            f"""SELECT bus, p_set FROM
                {sources['osm_curves']['schema']}.
                {sources['osm_curves']['table']}
                WHERE scn_name = '{scenario}'""",
            index_col='bus',
        )

    # Select data on industrial demands assigned to industrial sites

    ind_curves_sites = db.select_dataframe(
            f"""SELECT bus, p_set FROM
                {sources['sites_curves']['schema']}.
                {sources['sites_curves']['table']}
                WHERE scn_name = '{scenario}'""",
            index_col='bus',
        )

    # Create one df by appending all imported dataframes

    demand_curves = cts_curves.append([ind_curves_osm, ind_curves_sites])

    # Group all rows with the same bus

    demand_curves_bus = demand_curves.groupby(demand_curves.index).p_set.sum()

