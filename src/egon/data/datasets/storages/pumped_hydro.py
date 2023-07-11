"""
The module containing code allocating pumped hydro plants based on
data from MaStR and NEP.
"""

from geopy.geocoders import Nominatim
from sqlalchemy.orm import sessionmaker
import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets.chp.match_nep import match_nep_chp
from egon.data.datasets.chp.small_chp import assign_use_case
from egon.data.datasets.mastr import WORKING_DIR_MASTR_OLD
from egon.data.datasets.power_plants import (
    assign_bus_id,
    assign_voltage_level,
    filter_mastr_geometry,
    select_target,
)
import egon.data.config


def select_nep_pumped_hydro(scn):
    """Select pumped hydro plants from NEP power plants list


    Returns
    -------
    pandas.DataFrame
        Pumped hydro plants from NEP list
    """
    cfg = egon.data.config.datasets()["power_plants"]

    carrier = "pumped_hydro"

    if scn == "eGon2035":
        # Select plants with geolocation from list of conventional power plants
        nep_ph = db.select_dataframe(
            f"""
            SELECT bnetza_id, name, carrier, postcode, capacity, city,
            federal_state, c2035_capacity
            FROM {cfg['sources']['nep_conv']}
            WHERE carrier = '{carrier}'
            AND c2035_capacity > 0
            AND postcode != 'None';
            """
        )
        nep_ph.rename(
            columns={"c2035_capacity": "elec_capacity"}, inplace=True
        )
    elif scn == "status2019":
        # Select plants with geolocation from list of conventional power plants
        nep_ph = db.select_dataframe(
            f"""
            SELECT bnetza_id, name, carrier, postcode, capacity, city,
            federal_state
            FROM {cfg['sources']['nep_conv']}
            WHERE carrier = '{carrier}'
            AND capacity > 0
            AND postcode != 'None'
            AND commissioned < '2020';
            """
        )
        nep_ph["elec_capacity"] = nep_ph["capacity"]
    else:
        raise SystemExit(f"{scn} not recognised")

    # Removing plants out of Germany
    nep_ph["postcode"] = nep_ph["postcode"].astype(str)
    nep_ph = nep_ph[~nep_ph["postcode"].str.contains("A")]
    nep_ph = nep_ph[~nep_ph["postcode"].str.contains("L")]
    nep_ph = nep_ph[~nep_ph["postcode"].str.contains("nan")]

    # Remove the subunits from the bnetza_id
    nep_ph["bnetza_id"] = nep_ph["bnetza_id"].str[0:7]

    return nep_ph


def select_mastr_pumped_hydro():
    """Select pumped hydro plants from MaStR


    Returns
    -------
    pandas.DataFrame
        Pumped hydro plants from MaStR
    """
    sources = egon.data.config.datasets()["power_plants"]["sources"]

    # Read-in data from MaStR
    mastr_ph = pd.read_csv(
        WORKING_DIR_MASTR_OLD / sources["mastr_storage"],
        delimiter=",",
        usecols=[
            "Nettonennleistung",
            "EinheitMastrNummer",
            "Kraftwerksnummer",
            "Technologie",
            "Postleitzahl",
            "Laengengrad",
            "Breitengrad",
            "EinheitBetriebsstatus",
            "LokationMastrNummer",
            "Ort",
            "Bundesland",
        ],
    )

    # Rename columns
    mastr_ph = mastr_ph.rename(
        columns={
            "Kraftwerksnummer": "bnetza_id",
            "Technologie": "carrier",
            "Postleitzahl": "plz",
            "Ort": "city",
            "Bundesland": "federal_state",
            "Nettonennleistung": "el_capacity",
        }
    )

    # Select only pumped hydro units
    mastr_ph = mastr_ph[mastr_ph.carrier == "Pumpspeicher"]

    # Select only pumped hydro units which are in operation
    mastr_ph = mastr_ph[mastr_ph.EinheitBetriebsstatus == "InBetrieb"]

    # Insert geometry column
    mastr_ph = mastr_ph[~(mastr_ph["Laengengrad"].isnull())]
    mastr_ph = gpd.GeoDataFrame(
        mastr_ph,
        geometry=gpd.points_from_xy(
            mastr_ph["Laengengrad"], mastr_ph["Breitengrad"]
        ),
    )

    # Drop rows without post code and update datatype of postcode
    mastr_ph = mastr_ph[~mastr_ph["plz"].isnull()]
    mastr_ph["plz"] = mastr_ph["plz"].astype(int)

    # Calculate power in MW
    mastr_ph.loc[:, "el_capacity"] *= 1e-3

    mastr_ph = mastr_ph.set_crs(4326)

    mastr_ph = mastr_ph[~(mastr_ph["federal_state"].isnull())]

    # Drop CHP outside of Germany/ outside the test mode area
    mastr_ph = filter_mastr_geometry(mastr_ph, federal_state=None)

    return mastr_ph


def match_storage_units(
    nep,
    mastr,
    matched,
    buffer_capacity=0.1,
    consider_location="plz",
    consider_carrier=True,
    consider_capacity=True,
    scn="eGon2035",
):
    """Match storage_units (in this case only pumped hydro) from MaStR
    to list of power plants from NEP

    Parameters
    ----------
    nep : pandas.DataFrame
        storage units from NEP which are not matched to MaStR
    mastr : pandas.DataFrame
        Pstorage_units from MaStR which are not matched to NEP
    matched : pandas.DataFrame
        Already matched storage_units
    buffer_capacity : float, optional
        Maximum difference in capacity in p.u. The default is 0.1.
    scn : string, optional
        Scenario name

    Returns
    -------
    matched : pandas.DataFrame
        Matched CHP
    mastr : pandas.DataFrame
        storage_units from MaStR which are not matched to NEP
    nep : pandas.DataFrame
        storage_units from NEP which are not matched to MaStR

    """

    list_federal_states = pd.Series(
        {
            "Hamburg": "HH",
            "Sachsen": "SN",
            "MecklenburgVorpommern": "MV",
            "Thueringen": "TH",
            "SchleswigHolstein": "SH",
            "Bremen": "HB",
            "Saarland": "SL",
            "Bayern": "BY",
            "BadenWuerttemberg": "BW",
            "Brandenburg": "BB",
            "Hessen": "HE",
            "NordrheinWestfalen": "NW",
            "Berlin": "BE",
            "Niedersachsen": "NI",
            "SachsenAnhalt": "ST",
            "RheinlandPfalz": "RP",
        }
    )

    carrier = "pumped_hydro"

    for index, row in nep[
        (nep["carrier"] == carrier) & (nep["postcode"] != "None")
    ].iterrows():
        # Select plants from MaStR that match carrier, PLZ
        # and have a similar capacity
        # Create a copy of all power plants from MaStR
        selected = mastr.copy()

        # Set capacity constraint using buffer
        if consider_capacity:
            selected = selected[
                (
                    selected.el_capacity
                    <= row["capacity"] * (1 + buffer_capacity)
                )
                & (
                    selected.el_capacity
                    >= row["capacity"] * (1 - buffer_capacity)
                )
            ]

        # Set geographic constraint, either choose power plants
        # with the same postcode, city or federal state
        if consider_location == "plz":
            selected = selected[
                selected.plz.astype(int).astype(str) == row["postcode"]
            ]
        elif consider_location == "city":
            selected = selected[selected.city == row.city.replace("\n", " ")]
        elif consider_location == "federal_state":
            selected.loc[:, "federal_state"] = list_federal_states[
                selected.federal_state
            ].values
            selected = selected[selected.federal_state == row.federal_state]

        # Set capacity constraint if selected
        if consider_carrier:
            selected = selected[selected.carrier == carrier]

        # If a plant could be matched, add this to matched
        if len(selected) > 0:
            matched = pd.concat(
                [
                    matched,
                    gpd.GeoDataFrame(
                        data={
                            "source": "MaStR scaled with NEP 2021 list",
                            "MaStRNummer": selected.EinheitMastrNummer.head(1),
                            "carrier": carrier,
                            "el_capacity": row.elec_capacity,
                            "scenario": scn,
                            "geometry": selected.geometry.head(1),
                            "voltage_level": selected.voltage_level.head(1),
                        }
                    ),
                ]
            )

            # Drop matched storage units from nep
            nep = nep.drop(index)

            # Drop matched storage units from MaStR list if the location is accurate
            if consider_capacity & consider_carrier:
                mastr = mastr.drop(selected.index)

    return matched, mastr, nep


def get_location(unmatched):
    """Gets a geolocation for units which couldn't be matched using MaStR data.
    Uses geolocator and the city name from NEP data to create longitude and
    latitude for a list of unmatched units.

    Parameters
    ----------
    unmatched : pandas.DataFrame
        storage units from NEP which are not matched to MaStR but containing
        a city information

    Returns
    -------
    unmatched: pandas.DataFrame
        Units for which no geolocation could be identified

    located : pandas.DataFrame
        Units with a geolocation based on their city information

    """

    geolocator = Nominatim(user_agent="egon_data")

    # Create array of cities
    cities = unmatched.city.values

    # identify longitude and latitude for all cities in the array
    for city in cities:
        lon = geolocator.geocode(city).longitude
        lat = geolocator.geocode(city).latitude

        # write information on lon and lat to df
        unmatched.loc[unmatched.city == city, "lon"] = lon
        unmatched.loc[unmatched.city == city, "lat"] = lat

    # Get a point geometry from lon and lat information
    unmatched["geometry"] = gpd.points_from_xy(unmatched.lon, unmatched.lat)
    unmatched.crs = "EPSG:4326"

    # Copy units with lon and lat to a new dataframe
    located = unmatched[
        ["bnetza_id", "name", "carrier", "city", "elec_capacity", "geometry"]
    ].copy()
    located.dropna(subset=["geometry"], inplace=True)

    # Rename columns for compatibility reasons
    located = located.rename(
        columns={"elec_capacity": "el_capacity", "bnetza_id": "MaStRNummer"}
    )
    located["scenario"] = "eGon2035"
    located["source"] = "NEP power plants geolocated using city"

    unmatched = unmatched.drop(located.index.values)

    return located, unmatched


def apply_voltage_level_thresholds(power_plants):
    """Assigns voltage level to power plants based on thresholds defined for
    the egon project.

    Parameters
    ----------
    power_plants : pandas.DataFrame
        Power plants and their electrical capacity
    Returns
    -------
    pandas.DataFrame
        Power plants including voltage_level
    """

    # Identify voltage_level for every power plant taking thresholds into
    # account which were defined in the eGon project. Existing entries on voltage
    # will be overwritten

    power_plants.loc[power_plants["el_capacity"] < 0.1, "voltage_level"] = 7
    power_plants.loc[power_plants["el_capacity"] > 0.1, "voltage_level"] = 6
    power_plants.loc[power_plants["el_capacity"] > 0.2, "voltage_level"] = 5
    power_plants.loc[power_plants["el_capacity"] > 5.5, "voltage_level"] = 4
    power_plants.loc[power_plants["el_capacity"] > 20, "voltage_level"] = 3
    power_plants.loc[power_plants["el_capacity"] > 120, "voltage_level"] = 1

    return power_plants
