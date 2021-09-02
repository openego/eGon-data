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
from egon.data.datasets.chp.match_nep import match_nep_chp, map_carrier_nep_mastr
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
    cfg = egon.data.config.datasets()["power_plants"]

    carrier = 'Mineralöl-\nprodukte'

    # Select plants with geolocation from list of conventional power plants
    NEP_data = db.select_dataframe(
        f"""
        SELECT bnetza_id, name, postcode, carrier, c2035_capacity  city,
        federal_state
        FROM {cfg['sources']['nep_conv']}
        WHERE carrier = '{carrier}'
        AND chp = 'Nein'
        AND c2035_chp = 'Nein'
        AND c2035_capacity > 0
        AND postcode != 'None';
        """
        )


    # Removing plants out of Germany
    NEP_data['postcode'] = NEP_data['postcode'].astype(str)
    NEP_data = NEP_data[ ~ NEP_data['postcode'].str.contains('A')]
    NEP_data = NEP_data[ ~ NEP_data['postcode'].str.contains('L')]
    NEP_data = NEP_data[ ~ NEP_data['postcode'].str.contains('nan') ]

    # Remove the subunits from the bnetza_id
    NEP_data['bnetza_id'] = NEP_data['bnetza_id'].str[0:7]

    # Update carrier to match to MaStR
    map_carrier = map_carrier_nep_mastr()
    NEP_data['carrier'] = map_carrier[NEP_data['carrier'].values].values


    return NEP_data


def select_no_chp_combustion_mastr(carrier):
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
    carrier = 'Mineraloelprodukte'
    cfg = egon.data.config.datasets()["power_plants"]
    # import data for MaStR
    mastr = db.select_geodataframe(
        f"""
        SELECT  "EinheitMastrNummer",
                el_capacity,
                ST_setSRID(geometry, 4326) as geometry,
                carrier,
                plz,
                city
            FROM {cfg['sources']['mastr_combustion_without_chp']}
            WHERE carrier = '{carrier}'
            AND el_capacity >= 1;
        """,
        index_col= None,
        geom_col='geometry',
        epsg = 4326
    )


    return mastr


def merge_nep_mastr():

    cfg = egon.data.config.datasets()["power_plants"]

    nep = select_nep_power_plants('Mineralöl-\nprodukte')
    mastr = select_no_chp_combustion_mastr('Mineraloelprodukte')

    #Assign voltage level to MaStR
    mastr['voltage_level'] = assign_voltage_level(
        mastr.rename({'el_capacity': 'Nettonennleistung'}, axis=1
                          ), cfg)

    # Initalize DataFrame for matching power plants
    combustion_NEP_matched = geopandas.GeoDataFrame(
        columns = [
            'carrier','chp','el_capacity', 'scenario','geometry',
            'MaStRNummer', 'source', 'voltage_level'])

    # Match combustion plants from NEP list using PLZ, carrier and capacity
    combustion_NEP_matched, mastr, nep = match_nep_chp(
        nep, mastr, combustion_NEP_matched, buffer_capacity=0.1)

    # Match CHP from NEP list using first 4 numbers of PLZ,
    # carrier and capacity
    combustion_NEP_matched, mastr, nep = match_nep_chp(
        nep, mastr, combustion_NEP_matched, buffer_capacity=0.1,
        consider_location='city')

