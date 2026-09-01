"""The central module containing all code dealing with the distribution and
allocation of data on conventional and renewable power plants.
"""

from pathlib import Path
import logging

from geoalchemy2 import Geometry
from shapely.geometry import Point
from sqlalchemy import BigInteger, Column, Float, Integer, Sequence, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db, logger
from egon.data.datasets import (
    Dataset,
    DatasetSources,
    DatasetTargets,
    wrapped_partial,
)
from egon.data.datasets.power_plants.conventional import (
    match_nep_no_chp,
    select_nep_power_plants,
    select_no_chp_combustion_mastr,
)
from egon.data.datasets.power_plants.mastr import (
    EgonPowerPlantsBiomass,
    EgonPowerPlantsHydro,
    EgonPowerPlantsPv,
    EgonPowerPlantsWind,
    import_mastr,
)
from egon.data.datasets.power_plants.pv_rooftop import pv_rooftop_per_mv_grid
from egon.data.datasets.power_plants.pv_rooftop_buildings import (
    pv_rooftop_to_buildings,
)
import egon.data.config
import egon.data.datasets.power_plants.assign_weather_data as assign_weather_data  # noqa: E501
import egon.data.datasets.power_plants.metadata as pp_metadata
import egon.data.datasets.power_plants.pv_ground_mounted as pv_ground_mounted
import egon.data.datasets.power_plants.wind_farms as wind_onshore
import egon.data.datasets.power_plants.wind_offshore as wind_offshore

Base = declarative_base()


class EgonPowerPlants(Base):
    __tablename__ = "egon_power_plants"
    __table_args__ = {"schema": "supply"}
    id = Column(BigInteger, Sequence("pp_seq"), primary_key=True)
    sources = Column(JSONB)
    source_id = Column(JSONB)
    carrier = Column(String)
    el_capacity = Column(Float)
    bus_id = Column(Integer)
    voltage_level = Column(Integer)
    weather_cell_id = Column(Integer)
    scenario = Column(String)
    geom = Column(Geometry("POINT", 4326), index=True)


def create_tables():
    """Create tables for power plant data
    Returns
    -------
    None.
    """

    target_string = PowerPlants.targets.tables["power_plants"]
    schema, table = target_string.split(".")

    # Tables for future scenarios
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    engine = db.engine()
    db.execute_sql(f"DROP TABLE IF EXISTS {schema}.{table}")

    db.execute_sql("""DROP SEQUENCE IF EXISTS pp_seq""")
    EgonPowerPlants.__table__.create(bind=engine, checkfirst=True)

    # Tables for status quo
    tables = [
        EgonPowerPlantsWind,
        EgonPowerPlantsPv,
        EgonPowerPlantsBiomass,
        EgonPowerPlantsHydro,
    ]
    for t in tables:
        db.execute_sql(f"""
            DROP TABLE IF EXISTS {t.__table_args__['schema']}.
            {t.__tablename__} CASCADE;
            """)
        t.__table__.create(bind=engine, checkfirst=True)


def scale_prox2now(df, target, level="federal_state"):
    """Scale installed capacities linear to status quo power plants

    Parameters
    ----------
    df : pandas.DataFrame
        Status Quo power plants
    target : pandas.Series
        Target values for future scenario
    level : str, optional
        Scale per 'federal_state' or 'country'. The default is 'federal_state'.

    Returns
    -------
    df : pandas.DataFrame
        Future power plants

    """
    if level == "federal_state":
        df.loc[:, "Nettonennleistung"] = (
            (
                df.groupby("Bundesland").Nettonennleistung.apply(
                    lambda grp: grp / grp.sum()
                )
            )
            .reset_index(level=[0])
            .Nettonennleistung
        )
        df["Nettonennleistung"] = df.apply(
            lambda x: x["Nettonennleistung"] * target[x["Bundesland"]], axis=1
        )
    else:
        df.loc[:, "Nettonennleistung"] = df.Nettonennleistung * (
            target / df.Nettonennleistung.sum()
        )

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

    return (
        pd.read_sql(
            f"""SELECT DISTINCT ON (b.gen)
                         REPLACE(REPLACE(b.gen, '-', ''), 'ü', 'ue') as state,
                         a.capacity
                         FROM {PowerPlants.sources.tables['capacities']} a,
                         {PowerPlants.sources.tables['geom_federal_states']} b
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


def filter_mastr_geometry(mastr, federal_state=None):
    """Filter data from MaStR by geometry

    Parameters
    ----------
    mastr : pandas.DataFrame
        All power plants listed in MaStR
    federal_state : str or None
        Name of federal state whose power plants are returned.
        If None, data for Germany is returned

    Returns
    -------
    mastr_loc : pandas.DataFrame
        Power plants listed in MaStR with geometry inside German boundaries

    """

    if type(mastr) == pd.core.frame.DataFrame:
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
    else:
        mastr_loc = mastr.copy()

    # Drop entries outside of germany or federal state
    if not federal_state:
        sql = f"SELECT geometry as geom FROM {PowerPlants.sources.tables['geom_germany']}"
    else:
        sql = f"""
        SELECT geometry as geom
        FROM boundaries.vg250_lan_union
        WHERE REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') = '{federal_state}'"""

    mastr_loc = (
        gpd.sjoin(
            gpd.read_postgis(sql, con=db.engine()).to_crs(4326),
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

    # import target values
    target = select_target("biomass", scenario)

    # import data for MaStR
    mastr = pd.read_csv(PowerPlants.sources.files["mastr_biomass"]).query(
        "EinheitBetriebsstatus=='InBetrieb'"
    )

    # Drop entries without federal state or 'AusschließlichWirtschaftszone'
    mastr = mastr[
        mastr.Bundesland.isin(
            pd.read_sql(
                f"""SELECT DISTINCT ON (gen)
        REPLACE(REPLACE(gen, '-', ''), 'ü', 'ue') as states
        FROM {PowerPlants.sources.tables['geom_federal_states']}""",
                con=db.engine(),
            ).states.values
        )
    ]

    # Scaling will be done per federal state for all scenarios(former just in case of eGon2035 scenario)
    level = "federal_state"

    # Choose only entries with valid geometries inside DE/test mode
    mastr_loc = filter_mastr_geometry(mastr).set_geometry("geometry")

    # Scale capacities to meet target values
    mastr_loc = scale_prox2now(mastr_loc, target, level=level)

    # Assign bus_id
    if len(mastr_loc) > 0:
        mastr_loc["voltage_level"] = assign_voltage_level(
            mastr_loc, PowerPlants.sources.files
        )
        mastr_loc = assign_bus_id(mastr_loc, PowerPlants.sources.tables)

    # Insert entries with location
    session = sessionmaker(bind=db.engine())()

    nep_version = "NEP 2021" if scenario == "eGon2035" else "NEP 2025"

    for i, row in mastr_loc.iterrows():
        if not row.ThermischeNutzleistung > 0:
            entry = EgonPowerPlants(
                sources={"el_capacity": f"MaStR scaled with {nep_version}"},
                source_id={"MastrNummer": row.EinheitMastrNummer},
                carrier="biomass",
                el_capacity=row.Nettonennleistung,
                scenario=scenario,
                bus_id=row.bus_id,
                voltage_level=row.voltage_level,
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
    # Map MaStR carriers to eGon carriers
    map_carrier = {
        "run_of_river": ["Laufwasseranlage"],
        "reservoir": ["Speicherwasseranlage"],
    }

    nep_version = "NEP 2021" if scenario == "eGon2035" else "NEP 2025"

    for carrier in map_carrier.keys():
        # import target values
        target = select_target(carrier, scenario)

        # import data for MaStR
        mastr = pd.read_csv(PowerPlants.sources.files["mastr_hydro"]).query(
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
            FROM {PowerPlants.sources.tables['geom_federal_states']}""",
                    con=db.engine(),
                ).states.values
            )
        ]

        # Scaling will be done per federal state for all scenarios(former just in case of eGon2035 scenario)
        level = "federal_state" 

        # Scale capacities to meet target values
        mastr = scale_prox2now(mastr, target, level=level)

        # Choose only entries with valid geometries inside DE/test mode
        mastr_loc = filter_mastr_geometry(mastr).set_geometry("geometry")
        # TODO: Deal with power plants without geometry

        # Assign bus_id and voltage level
        if len(mastr_loc) > 0:
            mastr_loc["voltage_level"] = assign_voltage_level(
                mastr_loc,
                PowerPlants.sources.files,
            )
            mastr_loc = assign_bus_id(mastr_loc, PowerPlants.sources.tables)

        # Insert entries with location
        session = sessionmaker(bind=db.engine())()
        for i, row in mastr_loc.iterrows():
            entry = EgonPowerPlants(
                sources={"el_capacity": f"MaStR scaled with {nep_version}"},
                source_id={"MastrNummer": row.EinheitMastrNummer},
                carrier=carrier,
                el_capacity=row.Nettonennleistung,
                scenario=scenario,
                bus_id=row.bus_id,
                voltage_level=row.voltage_level,
                geom=f"SRID=4326;POINT({row.Laengengrad} {row.Breitengrad})",
            )
            session.add(entry)

        session.commit()


def assign_voltage_level(mastr_loc, sources):
    """Assigns voltage level to power plants.

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
    mastr_loc["Spannungsebene"] = np.nan
    mastr_loc["voltage_level"] = np.nan

    if "LokationMastrNummer" in mastr_loc.columns:
        # Adjust column names to format of MaStR location dataset
        cols = ["MaStRNummer", "Spannungsebene"]

        location = (
            pd.read_csv(
                PowerPlants.sources.files["mastr_location"],
                usecols=cols,
            )
            .rename(columns={"MaStRNummer": "LokationMastrNummer"})
            .set_index("LokationMastrNummer")
        )

        location = location[~location.index.duplicated(keep="first")]

        mastr_loc.loc[
            mastr_loc[
                mastr_loc.LokationMastrNummer.isin(location.index)
            ].index,
            "Spannungsebene",
        ] = location.Spannungsebene[
            mastr_loc[
                mastr_loc.LokationMastrNummer.isin(location.index)
            ].LokationMastrNummer
        ].values

        # Transfer voltage_level as integer from Spanungsebene
        map_voltage_levels = pd.Series(
            data={
                "Höchstspannung": 1,
                "Hoechstspannung": 1,
                "UmspannungZurHochspannung": 2,
                "Hochspannung": 3,
                "UmspannungZurMittelspannung": 4,
                "Mittelspannung": 5,
                "UmspannungZurNiederspannung": 6,
                "Niederspannung": 7,
            }
        )

        mastr_loc.loc[
            mastr_loc[mastr_loc["Spannungsebene"].notnull()].index,
            "voltage_level",
        ] = map_voltage_levels[
            mastr_loc.loc[
                mastr_loc[mastr_loc["Spannungsebene"].notnull()].index,
                "Spannungsebene",
            ].values
        ].values

    else:
        print(
            "No information about MaStR location available. "
            "All voltage levels are assigned using threshold values."
        )

    # If no voltage level is available from mastr, choose level according
    # to threshold values

    mastr_loc.voltage_level = assign_voltage_level_by_capacity(mastr_loc)

    return mastr_loc.voltage_level


def assign_voltage_level_by_capacity(mastr_loc):

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

        mastr_loc.loc[i, "voltage_level"] = level

    mastr_loc.voltage_level = mastr_loc.voltage_level.astype(int)

    return mastr_loc.voltage_level


def assign_bus_id(power_plants, sources, drop_missing=False):
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
        SELECT * FROM {PowerPlants.sources.tables['egon_mv_grid_district']}
        """,
        epsg=4326,
    )

    ehv_grid_districts = db.select_geodataframe(
        f"""
        SELECT * FROM {PowerPlants.sources.tables['ehv_voronoi']}
        """,
        epsg=4326,
    )

    # Assign power plants in hv and below to hvmv bus
    power_plants_hv = power_plants[power_plants.voltage_level >= 3].index
    if len(power_plants_hv) > 0:
        power_plants.loc[power_plants_hv, "bus_id"] = gpd.sjoin(
            power_plants[power_plants.index.isin(power_plants_hv)],
            mv_grid_districts,
        ).bus_id

    # Assign power plants in ehv to ehv bus
    power_plants_ehv = power_plants[power_plants.voltage_level < 3].index

    if len(power_plants_ehv) > 0:
        ehv_join = gpd.sjoin(
            power_plants[power_plants.index.isin(power_plants_ehv)],
            ehv_grid_districts,
        )

        if "bus_id_right" in ehv_join.columns:
            power_plants.loc[power_plants_ehv, "bus_id"] = gpd.sjoin(
                power_plants[power_plants.index.isin(power_plants_ehv)],
                ehv_grid_districts,
            ).bus_id_right

        else:
            power_plants.loc[power_plants_ehv, "bus_id"] = gpd.sjoin(
                power_plants[power_plants.index.isin(power_plants_ehv)],
                ehv_grid_districts,
            ).bus_id

    if drop_missing:
        power_plants = power_plants[~power_plants.bus_id.isnull()]

    # Assert that all power plants have a bus_id
    assert power_plants.bus_id.notnull().all(), f"""Some power plants are
    not attached to a bus: {power_plants[power_plants.bus_id.isnull()]}"""

    return power_plants


def insert_hydro_biomass():
    """Insert hydro and biomass power plants in database

    Returns
    -------
    None.

    """
    target_string = PowerPlants.targets.tables["power_plants"]
    schema, table = target_string.split(".")

    db.execute_sql(f"""
        DELETE FROM {schema}.{table}
        WHERE carrier IN ('biomass', 'reservoir', 'run_of_river')
        AND scenario IN ('eGon2035', 'reGon2037', 'reGon2045');
        """)

    s = egon.data.config.settings()["egon-data"]["--scenarios"]
    scenarios = []
    for scn in ["eGon2035", "reGon2037", "reGon2045"]:
        if scn in s:
            scenarios.append(scn)
            insert_biomass_plants(scn)

    for scenario in scenarios:
        insert_hydro_plants(scenario)


def allocate_conventional_non_chp_power_plants():
    """Allocate conventional power plants without CHPs based on the NEP target
    values and data from power plant registry (MaStR) by assigning them in a
    cascaded manner.

    Returns
    -------
    None.

    """
    future_scenarios = ["eGon2035", "reGon2037", "reGon2045"]
    active_scenarios = [
        scn for scn in future_scenarios
        if scn in egon.data.config.settings()["egon-data"]["--scenarios"]
    ]

    if not active_scenarios:
        return

    carriers = ["oil", "gas"]

    target_string = PowerPlants.targets.tables["power_plants"]
    schema, table = target_string.split(".")

    # Delete existing plants in the target table
    db.execute_sql(f"""
         DELETE FROM {schema}.{table}
         WHERE carrier IN ('gas', 'oil')
         AND scenario IN ({", ".join(f"'{s}'" for s in active_scenarios)});
         """)

    for scn in active_scenarios:
        for carrier in carriers:
            nep = select_nep_power_plants(carrier, scn)

            if nep.empty:
                print(f"DataFrame from NEP for carrier {carrier} is empty!")
                continue

            else:

                mastr = select_no_chp_combustion_mastr(carrier)

                # Assign voltage level to MaStR
                mastr["voltage_level"] = assign_voltage_level(
                    mastr.rename({"el_capacity": "Nettonennleistung"}, axis=1),
                    PowerPlants.sources.files,
                )

                # Initalize DataFrame for matching power plants
                matched = gpd.GeoDataFrame(
                    columns=[
                        "carrier",
                        "el_capacity",
                        "scenario",
                        "geometry",
                        "MaStRNummer",
                        "source",
                        "voltage_level",
                    ]
                )

                # Match combustion plants of a certain carrier from NEP list
                # using PLZ and capacity
                matched, mastr, nep = match_nep_no_chp(
                    nep,
                    mastr,
                    matched,
                    buffer_capacity=0.1,
                    consider_carrier=False,
                    scn=scn,
                )

                # Match plants from NEP list using city and capacity
                matched, mastr, nep = match_nep_no_chp(
                    nep,
                    mastr,
                    matched,
                    buffer_capacity=0.1,
                    consider_carrier=False,
                    consider_location="city",
                    scn=scn,
                )

                # Match plants from NEP list using plz,
                # neglecting the capacity
                matched, mastr, nep = match_nep_no_chp(
                    nep,
                    mastr,
                    matched,
                    consider_location="plz",
                    consider_carrier=False,
                    consider_capacity=False,
                    scn=scn,
                )

                # Match plants from NEP list using city,
                # neglecting the capacity
                matched, mastr, nep = match_nep_no_chp(
                    nep,
                    mastr,
                    matched,
                    consider_location="city",
                    consider_carrier=False,
                    consider_capacity=False,
                    scn=scn,
                )

                # Match remaining plants from NEP using the federal state
                matched, mastr, nep = match_nep_no_chp(
                    nep,
                    mastr,
                    matched,
                    buffer_capacity=0.1,
                    consider_location="federal_state",
                    consider_carrier=False,
                    scn=scn,
                )

                # Match remaining plants from NEP using the federal state
                matched, mastr, nep = match_nep_no_chp(
                    nep,
                    mastr,
                    matched,
                    buffer_capacity=0.7,
                    consider_location="federal_state",
                    consider_carrier=False,
                    scn=scn,
                )

                print(f"{matched.el_capacity.sum()} MW of {carrier} matched")
                print(f"{nep.elec_capacity.sum()} MW of {carrier} not matched")

                matched.crs = "EPSG:4326"

                # Assign bus_id
                # Load grid district polygons
                mv_grid_districts = db.select_geodataframe(
                    f"""
                SELECT * FROM {PowerPlants.sources.tables['egon_mv_grid_district']}
                """,
                    epsg=4326,
                )

                ehv_grid_districts = db.select_geodataframe(
                    f"""
                SELECT * FROM {PowerPlants.sources.tables['ehv_voronoi']}
                """,
                    epsg=4326,
                )

                # Perform spatial joins for plants in ehv and hv level seperately
                power_plants_hv = gpd.sjoin(
                    matched[matched.voltage_level >= 3],
                    mv_grid_districts[["bus_id", "geom"]],
                    how="left",
                ).drop(columns=["index_right"])
                power_plants_ehv = gpd.sjoin(
                    matched[matched.voltage_level < 3],
                    ehv_grid_districts[["bus_id", "geom"]],
                    how="left",
                ).drop(columns=["index_right"])

                # Combine both dataframes
                power_plants = pd.concat([power_plants_hv, power_plants_ehv])

                # Insert into target table
                session = sessionmaker(bind=db.engine())()
                for i, row in power_plants.iterrows():
                    entry = EgonPowerPlants(
                        sources={"el_capacity": row.source},
                        source_id={"MastrNummer": row.MaStRNummer},
                        carrier=row.carrier,
                        el_capacity=row.el_capacity,
                        voltage_level=row.voltage_level,
                        bus_id=row.bus_id,
                        scenario=row.scenario,
                        geom=f"SRID=4326;POINT({row.geometry.x} {row.geometry.y})",
                    )
                    session.add(entry)
                session.commit()


def allocate_other_power_plants():
    future_scenarios = ["eGon2035", "reGon2037", "reGon2045"]
    active_scenarios = [
        scn
        for scn in future_scenarios
        if scn in egon.data.config.settings()["egon-data"]["--scenarios"]
    ]

    if not active_scenarios:
        return

    boundary = egon.data.config.settings()["egon-data"]["--dataset-boundary"]

    target_string = PowerPlants.targets.tables["power_plants"]
    schema, table = target_string.split(".")

    db.execute_sql(f"""
        DELETE FROM {schema}.{table}
        WHERE carrier ='others'
        AND scenario IN ({", ".join(f"'{s}'" for s in active_scenarios)})
        """)

    # Select target values for carrier 'others' for each scenario
    for scenario in active_scenarios:
        # Select target values for carrier 'others'
        target = db.select_dataframe(f"""
            SELECT sum(capacity) as capacity, carrier, scenario_name, nuts
                FROM {PowerPlants.sources.tables['capacities']}
                WHERE scenario_name = '{scenario}'
                AND carrier = 'others'
                GROUP BY carrier, nuts, scenario_name;
            """)

        # Assign name of federal state
        map_states = {
            "DE1": "BadenWuerttemberg",
            "DEA": "NordrheinWestfalen",
            "DE7": "Hessen",
            "DE4": "Brandenburg",
            "DE5": "Bremen",
            "DEB": "RheinlandPfalz",
            "DEE": "SachsenAnhalt",
            "DEF": "SchleswigHolstein",
            "DE8": "MecklenburgVorpommern",
            "DEG": "Thueringen",
            "DE9": "Niedersachsen",
            "DED": "Sachsen",
            "DE6": "Hamburg",
            "DEC": "Saarland",
            "DE3": "Berlin",
            "DE2": "Bayern",
        }

        target = (
            target.replace({"nuts": map_states})
            .rename(columns={"nuts": "Bundesland"})
            .set_index("Bundesland")
        )
        target = target.capacity

        # Select 'non chp' power plants from mastr table
        mastr_combustion = select_no_chp_combustion_mastr("others")

        # Rename columns
        mastr_combustion = mastr_combustion.rename(
            columns={
                "carrier": "Energietraeger",
                "plz": "Postleitzahl",
                "city": "Ort",
                "federal_state": "Bundesland",
                "el_capacity": "Nettonennleistung",
            }
        )

        # Select power plants representing carrier 'others' from MaStR files
        mastr_sludge = pd.read_csv(PowerPlants.sources.files["mastr_gsgk"]).query(
            """EinheitBetriebsstatus=='InBetrieb' and Energietraeger=='Klärschlamm'"""
        )
        mastr_geothermal = pd.read_csv(
            PowerPlants.sources.files["mastr_gsgk"]
        ).query(
            "EinheitBetriebsstatus=='InBetrieb' and Energietraeger=='Geothermie' "
            "and Technologie == 'ORC (Organic Rankine Cycle)-Anlage'"
        )

        mastr_sg = pd.concat([mastr_sludge, mastr_geothermal])

        # Insert geometry column
        mastr_sg = mastr_sg[~(mastr_sg["Laengengrad"].isnull())]
        mastr_sg = gpd.GeoDataFrame(
            mastr_sg,
            geometry=gpd.points_from_xy(
                mastr_sg["Laengengrad"], mastr_sg["Breitengrad"], crs=4326
            ),
        )

        # Exclude columns which are not essential
        mastr_sg = mastr_sg.filter(
            [
                "EinheitMastrNummer",
                "Nettonennleistung",
                "geometry",
                "Energietraeger",
                "Postleitzahl",
                "Ort",
                "Bundesland",
            ],
            axis=1,
        )
        # Rename carrier
        mastr_sg.Energietraeger = "others"

        # Change data type
        mastr_sg["Postleitzahl"] = mastr_sg["Postleitzahl"].astype(int)

        # Capacity in MW
        mastr_sg.loc[:, "Nettonennleistung"] *= 1e-3

        # Merge different sources to one df
        mastr_others = pd.concat([mastr_sg, mastr_combustion]).reset_index()

        # Delete entries outside Schleswig-Holstein for test mode
        if boundary == "Schleswig-Holstein":
            mastr_others = mastr_others[
                mastr_others["Bundesland"] == "SchleswigHolstein"
            ]

        # Scale capacities prox to now to meet target values
        mastr_prox = scale_prox2now(mastr_others, target, level="federal_state")

        # Assign voltage_level based on scaled capacity
        mastr_prox["voltage_level"] = np.nan
        mastr_prox["voltage_level"] = assign_voltage_level_by_capacity(mastr_prox)

        # Rename columns
        mastr_prox = mastr_prox.rename(
            columns={
                "Energietraeger": "carrier",
                "Postleitzahl": "plz",
                "Ort": "city",
                "Bundesland": "federal_state",
                "Nettonennleistung": "el_capacity",
            }
        )

        # Assign bus_id
        mastr_prox = assign_bus_id(mastr_prox, PowerPlants.sources.tables)
        mastr_prox = mastr_prox.set_crs(4326, allow_override=True)

        # Insert into target table
        nep_version = "NEP 2021" if scenario == "eGon2035" else "NEP 2025"

        session = sessionmaker(bind=db.engine())()
        for i, row in mastr_prox.iterrows():
            entry = EgonPowerPlants(
                sources={"el_capacity": f"MaStR scaled with {nep_version}"},
                source_id={"MastrNummer": row.EinheitMastrNummer},
                carrier=row.carrier,
                el_capacity=row.el_capacity,
                voltage_level=row.voltage_level,
                bus_id=row.bus_id,
                scenario=scenario,
                geom=f"SRID=4326; {row.geometry}",
            )
            session.add(entry)
        session.commit()


def discard_not_available_generators(gen, max_date):
    # map column names in case they are still in German
    if (
        ("DatumEndgueltigeStilllegung" in gen.columns)
        | ("Inbetriebnahmedatum" in gen.columns)
        | ("DatumEndgueltigeStilllegung" in gen.columns)
    ):
        gen.rename(
            columns={
                "DatumEndgueltigeStilllegung": "decommissioning_date",
                "Inbetriebnahmedatum": "commissioning_date",
                "EinheitBetriebsstatus": "status",
            },
            inplace=True,
        )

    gen["decommissioning_date"] = pd.to_datetime(gen["decommissioning_date"])
    gen["commissioning_date"] = pd.to_datetime(gen["commissioning_date"])

    gen["decommissioning_date"] = pd.to_datetime(gen["decommissioning_date"])
    gen["commissioning_date"] = pd.to_datetime(gen["commissioning_date"])
    # drop plants that are commissioned after the max date
    gen = gen[gen["commissioning_date"] < max_date]

    # drop decommissioned plants while keeping the ones decommissioned
    # after the max date
    gen.loc[(gen["decommissioning_date"] > max_date), "status"] = "InBetrieb"
    gen = gen.loc[
        gen["status"].isin(["InBetrieb", "VoruebergehendStillgelegt"])
    ]
    # drop unnecessary columns
    gen = gen.drop(columns=["commissioning_date", "decommissioning_date"])

    return gen


def fill_missing_bus_and_geom(
    gens, carrier, geom_municipalities, mv_grid_districts
):
    # drop generators without data to get geometry.
    drop_id = gens[
        (gens.geom.is_empty) & ~(gens.location.isin(geom_municipalities.index))
    ].index
    new_geom = gens["capacity"][
        (gens.geom.is_empty) & (gens.location.isin(geom_municipalities.index))
    ]
    logger.info(
        f"""{len(drop_id)} {carrier} generator(s) ({int(gens.loc[drop_id, 'capacity']
        .sum())}MW) were drop"""
    )

    logger.info(f"""{len(new_geom)} {carrier} generator(s) ({int(new_geom
        .sum())}MW) received a geom based on location
          """)
    gens.drop(index=drop_id, inplace=True)

    # assign missing geometries based on location and buses based on geom

    gens["geom"] = gens.apply(
        lambda x: (
            geom_municipalities.at[x["location"], "geom"]
            if x["geom"].is_empty
            else x["geom"]
        ),
        axis=1,
    )
    gens["bus_id"] = gens.sjoin(
        mv_grid_districts[["bus_id", "geom"]], how="left"
    ).bus_id_right.values

    gens = gens.dropna(subset=["bus_id"])
    # convert geom to WKB
    gens["geom"] = gens["geom"].to_wkt()

    return gens


def power_plants_status_quo(scn_name="status2024"):
    def convert_master_info(df):
        # Add further information
        df["sources"] = [{"el_capacity": "MaStR"}] * df.shape[0]
        df["source_id"] = df["gens_id"].apply(lambda x: {"MastrNummer": x})
        return df

    def log_insert_capacity(df, tech):
        logger.info(f"""
            {len(df)} {tech} generators with a total installed capacity of
            {int(df["el_capacity"].sum())} MW were inserted into the db
              """)

    con = db.engine()

    target_string = PowerPlants.targets.tables["power_plants"]
    schema, table = target_string.split(".")

    max_date = pd.Timestamp(year=int(scn_name[-4:]), month=12, day=31)

    db.execute_sql(f"""
        DELETE FROM {schema}.{table}
        WHERE carrier IN ('wind_onshore', 'solar', 'biomass',
                          'run_of_river', 'reservoir', 'solar_rooftop',
                          'wind_offshore', 'nuclear', 'coal', 'lignite', 'oil',
                          'gas')
        AND scenario = '{scn_name}'
        """)

    # import municipalities to assign missing geom and bus_id
    geom_municipalities = gpd.GeoDataFrame.from_postgis(
        """
        SELECT gen, ST_UNION(geometry) as geom
        FROM boundaries.vg250_gem
        GROUP BY gen
        """,
        con,
        geom_col="geom",
    ).set_index("gen")
    geom_municipalities["geom"] = geom_municipalities["geom"].centroid

    mv_grid_districts = gpd.GeoDataFrame.from_postgis(
        f"""
        SELECT * FROM {PowerPlants.sources.tables['egon_mv_grid_district']}
        """,
        con,
    )
    mv_grid_districts.geom = mv_grid_districts.geom.to_crs(4326)

    # Conventional non CHP
    #  ###################
    conv = get_conventional_power_plants_non_chp(scn_name)

    conv = fill_missing_bus_and_geom(
        conv, "conventional", geom_municipalities, mv_grid_districts
    )

    conv = conv.rename(columns={"capacity": "el_capacity"})

    # Write into DB
    with db.session_scope() as session:
        session.bulk_insert_mappings(
            EgonPowerPlants,
            conv.to_dict(orient="records"),
        )

    log_insert_capacity(conv, tech="conventional non chp")

    # Hydro Power Plants
    #  ###################
    hydro = gpd.GeoDataFrame.from_postgis(
        f"""SELECT *, city AS location FROM {PowerPlants.sources.tables['hydro']}
        WHERE plant_type IN ('Laufwasseranlage', 'Speicherwasseranlage')""",
        con,
        geom_col="geom",
    )

    hydro = fill_missing_bus_and_geom(
        hydro, "hydro", geom_municipalities, mv_grid_districts
    )

    hydro = convert_master_info(hydro)
    hydro = discard_not_available_generators(hydro, max_date)
    hydro["carrier"] = hydro["plant_type"].replace(
        to_replace={
            "Laufwasseranlage": "run_of_river",
            "Speicherwasseranlage": "reservoir",
        }
    )
    hydro["scenario"] = scn_name
    hydro = hydro.rename(columns={"capacity": "el_capacity"})
    hydro = hydro.drop(columns="id")

    # Write into DB
    with db.session_scope() as session:
        session.bulk_insert_mappings(
            EgonPowerPlants,
            hydro.to_dict(orient="records"),
        )

    log_insert_capacity(hydro, tech="hydro")

    # Biomass
    #  ###################
    biomass = gpd.GeoDataFrame.from_postgis(
        f"""SELECT *, city AS location FROM {PowerPlants.sources.tables['biomass']}""",
        con,
        geom_col="geom",
    )

    # drop chp generators
    biomass["th_capacity"] = biomass["th_capacity"].fillna(0)
    biomass = biomass[biomass.th_capacity == 0]

    biomass = discard_not_available_generators(biomass, max_date)

    biomass = fill_missing_bus_and_geom(
        biomass, "biomass", geom_municipalities, mv_grid_districts
    )

    biomass = convert_master_info(biomass)
    biomass["scenario"] = scn_name
    biomass["carrier"] = "biomass"
    biomass = biomass.rename(columns={"capacity": "el_capacity"})
    biomass = biomass.drop(columns="id")

    # Write into DB
    with db.session_scope() as session:
        session.bulk_insert_mappings(
            EgonPowerPlants,
            biomass.to_dict(orient="records"),
        )

    log_insert_capacity(biomass, tech="biomass")

    # Solar
    #  ###################
    solar = gpd.GeoDataFrame.from_postgis(
        f"""SELECT *, city AS location FROM {PowerPlants.sources.tables['pv']}
        WHERE site_type IN ('Freifläche',
        'Bauliche Anlagen (Hausdach, Gebäude und Fassade)') """,
        con,
        geom_col="geom",
    )
    map_solar = {
        "Freifläche": "solar",
        "Bauliche Anlagen (Hausdach, Gebäude und Fassade)": "solar_rooftop",
    }

    solar = discard_not_available_generators(solar, max_date)

    solar["carrier"] = solar["site_type"].replace(to_replace=map_solar)

    solar = fill_missing_bus_and_geom(
        solar, "solar", geom_municipalities, mv_grid_districts
    )

    solar = convert_master_info(solar)
    solar["scenario"] = scn_name
    solar = solar.rename(columns={"capacity": "el_capacity"})
    solar = solar.drop(columns="id")

    # Write into DB
    with db.session_scope() as session:
        session.bulk_insert_mappings(
            EgonPowerPlants,
            solar.to_dict(orient="records"),
        )

    log_insert_capacity(solar, tech="solar")

    # Wind
    #  ###################
    wind_onshore = gpd.GeoDataFrame.from_postgis(
        f"""SELECT *, city AS location FROM {PowerPlants.sources.tables['wind']}""",
        con,
        geom_col="geom",
    )

    wind_onshore = discard_not_available_generators(wind_onshore, max_date)

    wind_onshore = fill_missing_bus_and_geom(
        wind_onshore, "wind_onshore", geom_municipalities, mv_grid_districts
    )

    wind_onshore = convert_master_info(wind_onshore)
    wind_onshore["scenario"] = scn_name
    wind_onshore = wind_onshore.rename(columns={"capacity": "el_capacity"})
    wind_onshore["carrier"] = "wind_onshore"
    wind_onshore = wind_onshore.drop(columns="id")

    # Write into DB
    with db.session_scope() as session:
        session.bulk_insert_mappings(
            EgonPowerPlants,
            wind_onshore.to_dict(orient="records"),
        )

    log_insert_capacity(wind_onshore, tech="wind_onshore")


def get_conventional_power_plants_non_chp(scn_name):
    max_date = pd.Timestamp(year=int(scn_name[-4:]), month=12, day=31)

    # Write conventional power plants in supply.egon_power_plants
    common_columns = [
        "EinheitMastrNummer",
        "Energietraeger",
        "Nettonennleistung",
        "Laengengrad",
        "Breitengrad",
        "Gemeinde",
        "Inbetriebnahmedatum",
        "EinheitBetriebsstatus",
        "DatumEndgueltigeStilllegung",
    ]
    # import nuclear power plants
    nuclear = pd.read_csv(
        PowerPlants.sources.files["mastr_nuclear"],
        usecols=common_columns,
    )
    # import combustion power plants
    comb = pd.read_csv(
        PowerPlants.sources.files["mastr_combustion"],
        usecols=common_columns + ["ThermischeNutzleistung"],
    )

    conv = pd.concat([comb, nuclear])

    conv = conv[
        conv.Energietraeger.isin(
            [
                "Braunkohle",
                "Mineralölprodukte",
                "Steinkohle",
                "Kernenergie",
                "Erdgas",
            ]
        )
    ]

    conv = discard_not_available_generators(conv, max_date)

    # convert from KW to MW
    conv["Nettonennleistung"] = conv["Nettonennleistung"] / 1000

    conv_cap_chp = (
        conv.groupby("Energietraeger")["Nettonennleistung"].sum() / 1e3
    )
    # drop chp generators
    conv["ThermischeNutzleistung"] = conv["ThermischeNutzleistung"].fillna(0)
    conv = conv[conv.ThermischeNutzleistung == 0]
    conv_cap_no_chp = (
        conv.groupby("Energietraeger")["Nettonennleistung"].sum() / 1e3
    )

    logger.info("Dropped CHP generators in GW")
    logger.info(conv_cap_chp - conv_cap_no_chp)

    # rename carriers
    conv["Energietraeger"] = conv["Energietraeger"].replace(
        to_replace={
            "Braunkohle": "lignite",
            "Steinkohle": "coal",
            "Erdgas": "gas",
            "Mineralölprodukte": "oil",
            "Kernenergie": "nuclear",
        }
    )

    # rename columns
    conv.rename(
        columns={
            "EinheitMastrNummer": "gens_id",
            "Energietraeger": "carrier",
            "Nettonennleistung": "capacity",
            "Gemeinde": "location",
        },
        inplace=True,
    )
    conv["bus_id"] = np.nan
    conv["geom"] = gpd.points_from_xy(
        conv.Laengengrad, conv.Breitengrad, crs=4326
    )
    conv.loc[(conv.Laengengrad.isna() | conv.Breitengrad.isna()), "geom"] = (
        Point()
    )
    conv = gpd.GeoDataFrame(conv, geometry="geom")

    # assign voltage level by capacity
    conv["voltage_level"] = np.nan
    conv["voltage_level"] = assign_voltage_level_by_capacity(
        conv.rename(columns={"capacity": "Nettonennleistung"})
    )
    # Add further information
    conv["sources"] = [{"el_capacity": "MaStR"}] * conv.shape[0]
    conv["source_id"] = conv["gens_id"].apply(lambda x: {"MastrNummer": x})
    conv["scenario"] = scn_name

    return conv


tasks = (
    create_tables,
    import_mastr,
)

for scn_name in egon.data.config.settings()["egon-data"]["--scenarios"]:
    if "status" in scn_name:
        tasks += (
            wrapped_partial(
                power_plants_status_quo,
                scn_name=scn_name,
                postfix=f"_{scn_name[-4:]}",
            ),
        )

if any(
    scn in egon.data.config.settings()["egon-data"]["--scenarios"]
    for scn in ["eGon2035", "reGon2037", "reGon2045"]
):
    tasks = tasks + (
        insert_hydro_biomass,
        allocate_conventional_non_chp_power_plants,
        allocate_other_power_plants,
        {
            wind_onshore.insert,
            pv_ground_mounted.insert,
            pv_rooftop_per_mv_grid,
        },
    )

tasks = tasks + (
    pv_rooftop_to_buildings,
    wind_offshore.insert,
)

for scn_name in egon.data.config.settings()["egon-data"]["--scenarios"]:
    tasks += (
        wrapped_partial(
            assign_weather_data.weatherId_and_busId,
            scn_name=scn_name,
            postfix=f"_{scn_name}",
        ),
    )

tasks += (pp_metadata.metadata,)


class PowerPlants(Dataset):
    sources = DatasetSources(
        tables={
            "geom_federal_states": "boundaries.vg250_lan",
            "geom_germany": "boundaries.vg250_sta_union",
            "egon_mv_grid_district": "grid.egon_mv_grid_district",
            "ehv_voronoi": "grid.egon_ehv_substation_voronoi",
            "capacities": "supply.egon_scenario_capacities",
            "hydro": "supply.egon_power_plants_hydro",
            "biomass": "supply.egon_power_plants_biomass",
            "pv": "supply.egon_power_plants_pv",
            "wind": "supply.egon_power_plants_wind",
            "mastr_combustion_without_chp": "supply.egon_mastr_conventional_without_chp",
            "nep_conv": "supply.egon_nep_conventional_powerplants",
            "buses_data": "osmtgmod_results.bus_data",
            "storages": "supply.egon_storages",
            "wind_potential_areas": "supply.egon_re_potential_area_wind",
            "hvmv_substation": "grid.egon_hvmv_substation",
            "electricity_demand": "demand.egon_demandregio_zensus_electricity",
            "map_zensus_grid_districts": "boundaries.egon_map_zensus_grid_districts",
            "map_grid_boundaries": "boundaries.egon_map_mvgriddistrict_vg250",
            "federal_states": "boundaries.vg250_lan",  # Alias
            "scenario_capacities": "supply.egon_scenario_capacities",  # Alias
            "weather_cells": "supply.egon_era5_weather_cells",
            "solar_feedin": "supply.egon_era5_renewable_feedin",
            "potential_area_pv_road_railway": "supply.egon_re_potential_area_pv_road_railway",
            "potential_area_pv_agriculture": "supply.egon_re_potential_area_pv_agriculture",
        },
        files={
            "mastr_biomass": "./bnetza_mastr/dump_2025-02-09/bnetza_mastr_biomass_cleaned.csv",
            "mastr_combustion": "./bnetza_mastr/dump_2025-02-09/bnetza_mastr_combustion_cleaned.csv",
            "mastr_gsgk": "./bnetza_mastr/dump_2025-02-09/bnetza_mastr_gsgk_cleaned.csv",
            "mastr_hydro": "./bnetza_mastr/dump_2025-02-09/bnetza_mastr_hydro_cleaned.csv",
            "mastr_location": "./bnetza_mastr/dump_2025-02-09/location_elec_generation_raw.csv",
            "mastr_nuclear": "./bnetza_mastr/dump_2025-02-09/bnetza_mastr_nuclear_cleaned.csv",
            "mastr_pv": "./bnetza_mastr/dump_2025-02-09/bnetza_mastr_solar_cleaned.csv",
            "mastr_storage": "./bnetza_mastr/dump_2025-02-09/bnetza_mastr_storage_cleaned.csv",
            "mastr_wind": "./bnetza_mastr/dump_2025-02-09/bnetza_mastr_wind_cleaned.csv",
            # --- Config/Meta values ---
            "osm_config": "https://download.geofabrik.de/europe/germany-240101.osm.pbf",
            "nep_2035": "NEP_V2021_scnC2035.xlsx",
            "nep_2037": "NEP_V2025_scnC2037.xlsx",
            "mastr_deposit_id": "14783581",
	    "wind_offshore_status2019": "windoffshore_status2019.xlsx",
            "data_bundle_deposit_id": "16576506",
            "status2024_date_max": "2024-12-31 23:59:00",
            "egon2021_date_max": "2021-12-31 23:59:00",
            "eGon2035_date_max": "2035-01-01",
            "mastr_geocoding_path": "mastr_geocoding",
        },
    )

    targets = DatasetTargets(
        tables={
            "power_plants": "supply.egon_power_plants",
            "generators": "grid.egon_etrago_generator",
            "generator_timeseries": "grid.egon_etrago_generator_timeseries",
            "mastr_geocoded": "supply.egon_mastr_geocoded",
            "power_plants_pv": "supply.egon_power_plants_pv",
            "power_plants_wind": "supply.egon_power_plants_wind",
            "power_plants_biomass": "supply.egon_power_plants_biomass",
            "power_plants_hydro": "supply.egon_power_plants_hydro",
            "power_plants_combustion": "supply.egon_power_plants_combustion",
            "power_plants_gsgk": "supply.egon_power_plants_gsgk",
            "power_plants_nuclear": "supply.egon_power_plants_nuclear",
            "power_plants_storage": "supply.egon_power_plants_storage",
        }
    )

    """
    This dataset deals with the distribution and allocation of power plants

    For the distribution and allocation of power plants to their corresponding
    grid connection point different technology-specific methods are applied.
    In a first step separate tables are created for wind, pv, hydro and biomass
    based power plants by running :py:func:`create_tables`.
    Different methods rely on the locations of existing power plants retrieved
    from the official power plant registry 'Marktstammdatenregister' applying
    function :py:func:`ìmport_mastr`.

    *Hydro and Biomass*
    Hydro and biomass power plants are distributed based on the status quo
    locations of existing power plants assuming that no further expansion of
    these technologies is to be expected in Germany. Hydro power plants include
    reservoir and run-of-river plants.
    Power plants without a correct geolocation are not taken into account.
    To compensate this, the installed capacities of the suitable plants are
    scaled up to meet the target value using function :py:func:`scale_prox2now`

    *Conventional power plants without CHP*
    The distribution of conventional plants, excluding CHPs, takes place in
    function :py:func:`allocate_conventional_non_chp_power_plants`. Therefore
    information about future power plants from the grid development plan
    function as the target value and are matched with actual existing power
    plants with correct geolocations from MaStR registry.

    *Wind onshore*


    *Wind offshore*

    *PV ground-mounted*

    *PV rooftop*

    *others*

    *Dependencies*
      * :py:class:`Chp <egon.data.datasets.chp.Chp>`
      * :py:class:`CtsElectricityDemand
      <egon.data.datasets.electricity_demand.CtsElectricityDemand>`
      * :py:class:`HouseholdElectricityDemand
      <egon.data.datasets.electricity_demand.HouseholdElectricityDemand>`
      * :py:class:`mastr_data <egon.data.datasets.mastr.mastr_data>`
      * :py:func:`define_mv_grid_districts
      <egon.data.datasets.mv_grid_districts.define_mv_grid_districts>`
      * :py:class:`RePotentialAreas
      <egon.data.datasets.re_potential_areas.RePotentialAreas>`
      * :py:class:`ZensusVg250
      <egon.data.datasets.RenewableFeedin>`
      * :py:class:`ScenarioCapacities
      <egon.data.datasets.scenario_capacities.ScenarioCapacities>`
      * :py:class:`ScenarioParameters
      <egon.data.datasets.scenario_parameters.ScenarioParameters>`
      * :py:func:`Setup <egon.data.datasets.database.setup>`
      * :py:class:`substation_extraction
      <egon.data.datasets.substation.substation_extraction>`
      * :py:class:`Vg250MvGridDistricts
      <egon.data.datasets.Vg250MvGridDistricts>`
      * :py:class:`ZensusMvGridDistricts
      <egon.data.datasets.zensus_mv_grid_districts.ZensusMvGridDistricts>`

    *Resulting tables*
      * :py:class:`supply.egon_power_plants
      <egon.data.datasets.power_plants.EgonPowerPlants>` is filled

    """

    #:
    name: str = "PowerPlants"
    #:
    version: str = "0.0.38"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=tasks,
        )
