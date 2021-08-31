"""
The module containing all code allocating power plants of different technologies
(waste, oil, gas, pumped_hydro, other_non_renewable, other_renewable) based on
data from MaStR and NEP.
"""

import pandas as pd
import geopandas
import egon.data.config
from egon.data import db, config
from egon.data.datasets.power_plants import (
    assign_voltage_level, assign_bus_id, assign_gas_bus_id,
    filter_mastr_geometry, select_target)
from egon.data.datasets.chp.match_nep import match_nep_chp
from egon.data.datasets.chp.small_chp import assign_use_case
from sqlalchemy.orm import sessionmaker


def select_nep_power_plants(carrier):
    """ Select waste power plants with location from NEP's list of power plants

    Parameters
    ----------
    carrier : str
        Name of energy carrier

    Returns
    -------
    pandas.DataFrame
        Waste power plants from NEP list

    """
    carrier = 'Abfall'
    cfg = egon.data.config.datasets()["power_plants"]

    # Select power plants from DB

    nep_plants = db.select_dataframe(
        f"""
        SELECT index, name, postcode, city, status, c2035_capacity
            FROM {cfg['sources']['nep_conv']}
            WHERE carrier = '{carrier}'
            AND chp = 'Nein'
            AND c2035_chp = 'Nein';
        """)
    return nep_plants


def select_combustion_mastr(carrier):
    """ Select power plants of a certain carrier from MaStR data which excludes
    all power plants used for allocation of CHP plants.

    Parameters
    ----------
    carrier : str
        Name of energy carrier

    Returns
    -------
    pandas.DataFrame
        Power plants from NEP list

    """

    carrier = 'NichtBiogenerAbfall'
    cfg = egon.data.config.datasets()["power_plants"]
    # import data for MaStR
    mastr = db.select_dataframe(
        f"""
        SELECT *
            FROM {cfg['sources']['mastr_combustion_without_chp']}
            WHERE carrier = '{carrier}';
        """
        )


