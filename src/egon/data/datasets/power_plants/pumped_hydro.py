"""
The module containing all code allocating power plants of different technologies
(waste, oil, gas, pumped_hydro, other_non_renewable, other_renewable) based on
data from MaStR and NEP.
"""

import pandas as pd
import geopandas as gpd
import egon.data.config
from egon.data import db, config
from egon.data.datasets.power_plants import (
    assign_voltage_level, assign_bus_id, assign_gas_bus_id,
    filter_mastr_geometry, select_target)
from egon.data.datasets.chp.match_nep import match_nep_chp, map_carrier_nep_mastr, map_carrier_egon_mastr, map_carrier_matching
from egon.data.datasets.chp.small_chp import assign_use_case
from sqlalchemy.orm import sessionmaker


def select_nep_pumped_hydro():
    """ Select pumped hydro plants from NEP power plants list


    Returns
    -------
    pandas.DataFrame
        Pumped hydro plants from NEP list
    """
    cfg = egon.data.config.datasets()["power_plants"]

    carrier = 'pumped_hydro'

    # Select plants with geolocation from list of conventional power plants
    nep_ph = db.select_dataframe(
        f"""
        SELECT bnetza_id, name, carrier, chp, postcode, capacity, city,
        federal_state, c2035_chp, c2035_capacity
        FROM {cfg['sources']['nep_conv']}
        WHERE carrier = '{carrier}'
        AND c2035_capacity > 0
        AND postcode != 'None';
        """
        )


    # Removing plants out of Germany
    nep_ph['postcode'] = nep_ph['postcode'].astype(str)
    nep_ph = nep_ph[ ~ nep_ph['postcode'].str.contains('A')]
    nep_ph = nep_ph[ ~ nep_ph['postcode'].str.contains('L')]
    nep_ph = nep_ph[ ~ nep_ph['postcode'].str.contains('nan') ]

    # Remove the subunits from the bnetza_id
    nep_ph['bnetza_id'] = nep_ph['bnetza_id'].str[0:7]


    return nep_ph

def select_mastr_pumped_hydro():
    """ Select pumped hydro plants from MaStR


    Returns
    -------
    pandas.DataFrame
        Pumped hydro plants from MaStR
    """
    sources = egon.data.config.datasets()["power_plants"]["sources"]

     # Read-in data from MaStR
    mastr_ph = pd.read_csv(
        sources["mastr_storage"],
        delimiter = ',',
        usecols = ['Nettonennleistung',
                    'EinheitMastrNummer',
                    'Kraftwerksnummer',
                    'Technologie',
                    'Postleitzahl',
                    'Laengengrad',
                    'Breitengrad',
                    'EinheitBetriebsstatus',
                    'LokationMastrNummer',
                    'Ort',
                    'Bundesland'])

    # Rename columns
    mastr_ph = mastr_ph.rename(columns={
        'Kraftwerksnummer': 'bnetza_id',
        'Technologie': 'carrier',
        'Postleitzahl': 'plz',
        'Ort': 'city',
        'Bundesland':'federal_state',
        'Nettonennleistung': 'el_capacity'})

    # Select only pumped hydro units
    mastr_ph = mastr_ph[mastr_ph.carrier=='Pumpspeicher']


    # Select only pumped hydro units which are in operation
    mastr_ph = mastr_ph[mastr_ph.EinheitBetriebsstatus=='InBetrieb']

    # Insert geometry column
    mastr_ph = mastr_ph[ ~ ( mastr_ph['Laengengrad'].isnull()) ]
    mastr_ph = gpd.GeoDataFrame(
        mastr_ph, geometry=gpd.points_from_xy(
            mastr_ph['Laengengrad'], mastr_ph['Breitengrad']))


    # Drop rows without post code and update datatype of postcode
    mastr_ph = mastr_ph[~mastr_ph['plz'].isnull()]
    mastr_ph['plz'] = mastr_ph['plz'].astype(int)

    # Calculate power in MW
    mastr_ph.loc[:, 'el_capacity'] *=1e-3

    mastr_ph = mastr_ph.set_crs(4326)

    # Drop CHP outside of Germany
    mastr_ph = filter_mastr_geometry(mastr_ph, federal_state=None)

    return mastr_ph
