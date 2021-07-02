"""The central module containing all code dealing with power plant data.
"""
from egon.data import db
from sqlalchemy import Column, String, Float, Integer, Sequence, Boolean, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry
import pandas as pd
import geopandas as gpd
import egon.data.config
from egon.data.datasets import Dataset
import numpy as np
from egon.data.datasets.power_plants.pv_rooftop import pv_rooftop_per_mv_grid
from egon.data.datasets.power_plants.wind_farms import wind_power_parks
from egon.data.datasets.power_plants.pv_ground_mounted import (
    regio_of_pv_ground_mounted)
Base = declarative_base()


class EgonPowerPlants(Base):
    __tablename__ = "egon_power_plants"
    __table_args__ = {"schema": "supply"}
    id = Column(BigInteger, Sequence("pp_seq"), primary_key=True)
    sources = Column(JSONB)
    source_id = Column(JSONB)
    carrier = Column(String)
    chp = Column(Boolean)
    el_capacity = Column(Float)
    th_capacity = Column(Float)
    bus_id = Column(Integer)
    voltage_level = Column(Integer)
    weather_cell_id = Column(Integer)
    scenario = Column(String)
    geom = Column(Geometry("POINT", 4326))

class PowerPlants(Dataset):

    def __init__(self, dependencies):
        super().__init__(
            name="PowerPlants",
            version="0.0.0-1",
            dependencies=dependencies,
            tasks=(
                create_tables,
                {insert_hydro_biomass,
                 wind_power_parks,
                 regio_of_pv_ground_mounted,
                 pv_rooftop_per_mv_grid
                 }
            ),
        )

def create_tables():
    """Create tables for power plant data
    Returns
    -------
    None.
    """

    cfg = egon.data.config.datasets()["power_plants"]
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {cfg['target']['schema']};")
    engine = db.engine()
    db.execute_sql(
        f"""DROP TABLE IF EXISTS
        {cfg['target']['schema']}.{cfg['target']['table']}"""
    )

    db.execute_sql(
        """DROP SEQUENCE IF EXISTS pp_seq"""
    )
    EgonPowerPlants.__table__.create(bind=engine, checkfirst=True)


def scale_prox2now(df, target, level="federal_state"):
    """Scale installed capacities linear to status quo power plants

    Parameters
    ----------
    df : pandas.DataFrame
        Status Quo power plants
    target : pandas.Series
        Target values for future sceanrio
    level : str, optional
        Scale per 'federal_state' or 'country'. The default is 'federal_state'.

    Returns
    -------
    df : pandas.DataFrame
        Future power plants

    """

    if level == "federal_state":
        df.loc[:, "Nettonennleistung"] = (
            df.groupby(df.Bundesland)
            .Nettonennleistung.apply(lambda grp: grp / grp.sum())
            .mul(target[df.Bundesland.values].values)
        )
    else:
        df.loc[:, "Nettonennleistung"] = df.Nettonennleistung.apply(
            lambda x: x / x.sum()
        ).mul(target.values)

    df = df[df.Nettonennleistung > 0]

    return df


def select_target(carrier, scenario):
    """Select installed capacity per scenario and carrier

    Parameters
    ----------
    carrier : str
        Name of energy carrier
    scenario : str
        Name of scenario

    Returns
    -------
    pandas.Series
        Target values for carrier and scenario

    """
    cfg = egon.data.config.datasets()["power_plants"]

    return (
        pd.read_sql(
            f"""SELECT DISTINCT ON (b.gen)
                         REPLACE(REPLACE(b.gen, '-', ''), 'ü', 'ue') as state,
                         a.capacity
                         FROM {cfg['sources']['capacities']} a,
                         {cfg['sources']['geom_federal_states']} b
                         WHERE a.nuts = b.nuts
                         AND scenario_name = '{scenario}'
                         AND carrier = '{carrier}'
                         AND b.gen NOT IN ('Baden-Württemberg (Bodensee)',
                                           'Bayern (Bodensee)')""",
            con=db.engine(),
        )
        .set_index("state")
        .capacity
    )


def filter_mastr_geometry(mastr):
    """Filter data from MaStR by geometry

    Parameters
    ----------
    mastr : pandas.DataFrame
        All power plants listed in MaStR

    Returns
    -------
    mastr_loc : pandas.DataFrame
        Power plants listed in MaStR with geometry inside German boundaries

    """
    cfg = egon.data.config.datasets()["power_plants"]

    # Drop entries without geometry for insert
    mastr_loc = mastr[
        mastr.Laengengrad.notnull() & mastr.Breitengrad.notnull()
    ]

    # Create geodataframe
    mastr_loc = gpd.GeoDataFrame(
        mastr_loc,
        geometry=gpd.points_from_xy(
            mastr_loc.Laengengrad, mastr_loc.Breitengrad, crs=4326
        ),
    )

    # Drop entries outside of germany
    mastr_loc = (
        gpd.sjoin(
            gpd.read_postgis(
                f"SELECT geometry as geom FROM {cfg['sources']['geom_germany']}",
                con=db.engine(),
            ).to_crs(4326),
            mastr_loc,
            how="right",
        )
        .query("index_left==0")
        .drop("index_left", axis=1)
    )

    return mastr_loc


def insert_biomass_plants(scenario):
    """Insert biomass power plants of future scenario

    Parameters
    ----------
    scenario : str
        Name of scenario.

    Returns
    -------
    None.

    """
    cfg = egon.data.config.datasets()["power_plants"]

    # import target values from NEP 2021, scneario C 2035
    target = select_target("biomass", scenario)

    # import data for MaStR
    mastr = pd.read_csv(cfg["sources"]["mastr_biomass"]).query(
        "EinheitBetriebsstatus=='InBetrieb'"
    )

    # Drop entries without federal state or 'AusschließlichWirtschaftszone'
    mastr = mastr[
        mastr.Bundesland.isin(
            pd.read_sql(
                f"""SELECT DISTINCT ON (gen)
        REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') as states
        FROM {cfg['sources']['geom_federal_states']}""",
                con=db.engine(),
            ).states.values
        )
    ]

    # Scaling will be done per federal state in case of eGon2035 scenario.
    if scenario == "eGon2035":
        level = "federal_state"
    else:
        level = "country"

    # Scale capacities to meet target values
    mastr = scale_prox2now(mastr, target, level=level)

    # Choose only entries with valid geometries inside DE/test mode
    mastr_loc = filter_mastr_geometry(mastr).set_geometry('geometry')
    # TODO: Deal with power plants without geometry

    # Assign bus_id
    if len(mastr_loc) > 0:
        mastr_loc['voltage_level'] = assign_voltage_level(mastr_loc, cfg)
        mastr_loc = assign_bus_id(mastr_loc, cfg)

    # Insert entries with location
    session = sessionmaker(bind=db.engine())()
    for i, row in mastr_loc.iterrows():
        entry = EgonPowerPlants(
            sources={
                "chp": "MaStR",
                "el_capacity": "MaStR scaled with NEP 2021",
                "th_capacity": "MaStR",
            },
            source_id={"MastrNummer": row.EinheitMastrNummer},
            carrier="biomass",
            chp=type(row.KwkMastrNummer) != float,
            el_capacity=row.Nettonennleistung,
            th_capacity=row.ThermischeNutzleistung / 1000,
            scenario=scenario,
            bus_id = row.bus_id,
            voltage_level = row.voltage_level,
            geom=f"SRID=4326;POINT({row.Laengengrad} {row.Breitengrad})",
        )
        session.add(entry)

    session.commit()


def insert_hydro_plants(scenario):
    """Insert hydro power plants of future scenario.

    Hydro power plants are diveded into run_of_river and reservoir plants
    according to Marktstammdatenregister.
    Additional hydro technologies (e.g. turbines inside drinking water
    systems) are not considered.

    Parameters
    ----------
    scenario : str
        Name of scenario.

    Returns
    -------
    None.

    """
    cfg = egon.data.config.datasets()["power_plants"]

    # Map MaStR carriers to eGon carriers
    map_carrier = {
        "run_of_river": ["Laufwasseranlage"],
        "reservoir": ["Speicherwasseranlage"],
    }

    for carrier in map_carrier.keys():

        # import target values
        target = select_target(carrier, scenario)

        # import data for MaStR
        mastr = pd.read_csv(cfg["sources"]["mastr_hydro"]).query(
            "EinheitBetriebsstatus=='InBetrieb'"
        )

        # Choose only plants with specific carriers
        mastr = mastr[mastr.ArtDerWasserkraftanlage.isin(map_carrier[carrier])]

        # Drop entries without federal state or 'AusschließlichWirtschaftszone'
        mastr = mastr[
            mastr.Bundesland.isin(
                pd.read_sql(
                    f"""SELECT DISTINCT ON (gen)
            REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') as states
            FROM {cfg['sources']['geom_federal_states']}""",
                    con=db.engine(),
                ).states.values
            )
        ]

        # Scaling will be done per federal state in case of eGon2035 scenario.
        if scenario == "eGon2035":
            level = "federal_state"
        else:
            level = "country"

        # Scale capacities to meet target values
        mastr = scale_prox2now(mastr, target, level=level)

        # Choose only entries with valid geometries inside DE/test mode
        mastr_loc = filter_mastr_geometry(mastr).set_geometry('geometry')
        # TODO: Deal with power plants without geometry

        # Assign bus_id and voltage level
        if len(mastr_loc) > 0:
            mastr_loc['voltage_level'] = assign_voltage_level(mastr_loc, cfg)
            mastr_loc = assign_bus_id(mastr_loc, cfg)

        # Insert entries with location
        session = sessionmaker(bind=db.engine())()
        for i, row in mastr_loc.iterrows():
            entry = EgonPowerPlants(
                sources={
                    "chp": "MaStR",
                    "el_capacity": "MaStR scaled with NEP 2021",
                },
                source_id={"MastrNummer": row.EinheitMastrNummer},
                carrier=carrier,
                chp=type(row.KwkMastrNummer) != float,
                el_capacity=row.Nettonennleistung,
                scenario=scenario,
                bus_id = row.bus_id,
                voltage_level = row.voltage_level,
                geom=f"SRID=4326;POINT({row.Laengengrad} {row.Breitengrad})",
            )
            session.add(entry)

        session.commit()

def assign_voltage_level(mastr_loc, cfg):
    """ Assigns voltage level to power plants.

    If location data inluding voltage level is available from
    Marktstammdatenregister, this is used. Otherwise the voltage level is
    assigned according to the electrical capacity.

    Parameters
    ----------
    mastr_loc : pandas.DataFrame
        Power plants listed in MaStR with geometry inside German boundaries

    Returns
    -------
    pandas.DataFrame
        Power plants including voltage_level

    """
    mastr_loc['Spannungsebene'] = np.nan
    mastr_loc['voltage_level'] = np.nan

    location = pd.read_csv(cfg['sources']['mastr_location'], usecols=[
            'LokationMastrNummer', 'Spannungsebene']).set_index(
                'LokationMastrNummer')

    location = location[~location.index.duplicated(keep='first')]

    mastr_loc.loc[mastr_loc[mastr_loc.LokationMastrNummer.isin(
        location.index)].index,'Spannungsebene'] = location.Spannungsebene[
        mastr_loc[mastr_loc.LokationMastrNummer.isin(
        location.index)].LokationMastrNummer].values

    # Transfer voltage_level as integer from Spanungsebene
    map_voltage_levels=pd.Series(data={
        'Höchstspannung': 1,
        'Hoechstspannung': 1,
        'Hochspannung': 3,
        'UmspannungZurMittelspannung': 4,
        'Mittelspannung': 5,
        'UmspannungZurNiederspannung': 6,
        'Niederspannung':7})

    mastr_loc.loc[mastr_loc[mastr_loc['Spannungsebene'].notnull()].index,
              'voltage_level'] = map_voltage_levels[
        mastr_loc.loc[mastr_loc[mastr_loc['Spannungsebene'].notnull()].index,
              'Spannungsebene'].values].values


    # If no voltage level is available from mastr, choose level according
    # to threshold values

    for i, row in mastr_loc[mastr_loc.voltage_level.isnull()].iterrows():

        if row.Nettonennleistung > 120:
            level = 1
        elif row.Nettonennleistung > 20:
            level = 3
        elif row.Nettonennleistung > 5.5:
            level = 4
        elif row.Nettonennleistung > 0.2:
            level = 5
        elif row.Nettonennleistung > 0.1:
            level = 6
        else:
            level = 7

        mastr_loc.loc[i, 'voltage_level'] = level

    mastr_loc.voltage_level = mastr_loc.voltage_level.astype(int)

    return mastr_loc.voltage_level

def assign_bus_id(power_plants, cfg):
    """Assigns bus_ids to power plants according to location and voltage level

    Parameters
    ----------
    power_plants : pandas.DataFrame
        Power plants including voltage level

    Returns
    -------
    power_plants : pandas.DataFrame
        Power plants including voltage level and bus_id

    """

    mv_grid_districts = db.select_geodataframe(
        f"""
        SELECT * FROM {cfg['sources']['mv_grid_districts']}
        """, epsg=4326)

    ehv_grid_districts = db.select_geodataframe(
        f"""
        SELECT * FROM {cfg['sources']['ehv_voronoi']}
        """, epsg=4326)

    # Assign power plants in hv and below to hvmv bus
    power_plants_hv = power_plants[power_plants.voltage_level >= 3].index
    if len(power_plants_hv) > 0:
        power_plants.loc[power_plants_hv, 'bus_id'] = gpd.sjoin(
            power_plants[power_plants.index.isin(power_plants_hv)
                         ], mv_grid_districts).subst_id

    # Assign power plants in ehv to ehv bus
    power_plants_ehv = power_plants[power_plants.voltage_level < 3].index

    if len(power_plants_ehv) > 0:
        power_plants.loc[power_plants_ehv, 'bus_id'] = gpd.sjoin(
            power_plants[power_plants.index.isin(power_plants_ehv)
                         ], ehv_grid_districts).bus_id_right

    # Assert that all power plants have a bus_id
    assert power_plants.bus_id.notnull().all(), """Some power plants are
    not attached to a bus."""

    return power_plants

def insert_hydro_biomass():
    """Insert hydro and biomass power plants in database

    Returns
    -------
    None.

    """
    cfg = egon.data.config.datasets()["power_plants"]
    db.execute_sql(
        f"""
        DELETE FROM {cfg['target']['schema']}.{cfg['target']['table']}
        WHERE carrier IN ('biomass', 'reservoir', 'run_of_river')
        """
    )

    for scenario in ["eGon2035"]:
        insert_biomass_plants(scenario)
        insert_hydro_plants(scenario)


