"""The central module containing all code dealing with power plant data.
"""
from geoalchemy2 import Geometry
from sqlalchemy import BigInteger, Column, Float, Integer, Sequence, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.mastr import (
    WORKING_DIR_MASTR_NEW,
    WORKING_DIR_MASTR_OLD,
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
    geocode_mastr_data,
    pv_rooftop_to_buildings,
)
import egon.data.config
import egon.data.datasets.power_plants.assign_weather_data as assign_weather_data  # noqa: E501
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


class PowerPlants(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="PowerPlants",
            version="0.0.18",
            dependencies=dependencies,
            tasks=(
                create_tables,
                import_mastr,
                power_plants_status_quo,
                insert_hydro_biomass,
                allocate_conventional_non_chp_power_plants,
                allocate_other_power_plants,
                {
                    wind_onshore.insert,
                    pv_ground_mounted.insert,
                    (
                        pv_rooftop_per_mv_grid,
                        geocode_mastr_data,
                        pv_rooftop_to_buildings,
                    ),
                },
                wind_offshore.insert,
                assign_weather_data.weatherId_and_busId,
            ),
        )


def create_tables():
    """Create tables for power plant data
    Returns
    -------
    None.
    """

    # Tables for future scenarios
    cfg = egon.data.config.datasets()["power_plants"]
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {cfg['target']['schema']};")
    engine = db.engine()
    db.execute_sql(
        f"""DROP TABLE IF EXISTS
        {cfg['target']['schema']}.{cfg['target']['table']}"""
    )

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
        db.execute_sql(
            f"DROP TABLE IF EXISTS {t.__table_args__['schema']}.{t.__tablename__} CASCADE;"
        )
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


def filter_mastr_geometry(mastr, federal_state=None):
    """Filter data from MaStR by geometry

    Parameters
    ----------
    mastr : pandas.DataFrame
        All power plants listed in MaStR
    federal_state : str or None
        Name of federal state whoes power plants are returned.
        If None, data for Germany is returned

    Returns
    -------
    mastr_loc : pandas.DataFrame
        Power plants listed in MaStR with geometry inside German boundaries

    """
    cfg = egon.data.config.datasets()["power_plants"]

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
        sql = f"SELECT geometry as geom FROM {cfg['sources']['geom_germany']}"
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
    cfg = egon.data.config.datasets()["power_plants"]

    # import target values from NEP 2021, scneario C 2035
    target = select_target("biomass", scenario)

    # import data for MaStR
    mastr = pd.read_csv(
        WORKING_DIR_MASTR_OLD / cfg["sources"]["mastr_biomass"]
    ).query("EinheitBetriebsstatus=='InBetrieb'")

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

    # Choose only entries with valid geometries inside DE/test mode
    mastr_loc = filter_mastr_geometry(mastr).set_geometry("geometry")

    # Scale capacities to meet target values
    mastr_loc = scale_prox2now(mastr_loc, target, level=level)

    # Assign bus_id
    if len(mastr_loc) > 0:
        mastr_loc["voltage_level"] = assign_voltage_level(
            mastr_loc, cfg, WORKING_DIR_MASTR_OLD
        )
        mastr_loc = assign_bus_id(mastr_loc, cfg)

    # Insert entries with location
    session = sessionmaker(bind=db.engine())()

    for i, row in mastr_loc.iterrows():
        if not row.ThermischeNutzleistung > 0:
            entry = EgonPowerPlants(
                sources={"el_capacity": "MaStR scaled with NEP 2021"},
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
        mastr = pd.read_csv(
            WORKING_DIR_MASTR_NEW / cfg["sources"]["mastr_hydro"]
        ).query("EinheitBetriebsstatus=='InBetrieb'")

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
        mastr_loc = filter_mastr_geometry(mastr).set_geometry("geometry")
        # TODO: Deal with power plants without geometry

        # Assign bus_id and voltage level
        if len(mastr_loc) > 0:
            mastr_loc["voltage_level"] = assign_voltage_level(
                mastr_loc, cfg, WORKING_DIR_MASTR_NEW
            )
            mastr_loc = assign_bus_id(mastr_loc, cfg)

        # Insert entries with location
        session = sessionmaker(bind=db.engine())()
        for i, row in mastr_loc.iterrows():
            entry = EgonPowerPlants(
                sources={"el_capacity": "MaStR scaled with NEP 2021"},
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


def assign_voltage_level(mastr_loc, cfg, mastr_working_dir):
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
        if mastr_working_dir == WORKING_DIR_MASTR_OLD:
            cols = ["LokationMastrNummer", "Spannungsebene"]
        elif mastr_working_dir == WORKING_DIR_MASTR_NEW:
            cols = ["MaStRNummer", "Spannungsebene"]
        else:
            raise ValueError("Invalid MaStR working directory!")

        location = (
            pd.read_csv(
                mastr_working_dir / cfg["sources"]["mastr_location"],
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
        SELECT * FROM {cfg['sources']['egon_mv_grid_district']}
        """,
        epsg=4326,
    )

    ehv_grid_districts = db.select_geodataframe(
        f"""
        SELECT * FROM {cfg['sources']['ehv_voronoi']}
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


def allocate_conventional_non_chp_power_plants():
    carrier = ["oil", "gas"]

    cfg = egon.data.config.datasets()["power_plants"]

    # Delete existing plants in the target table
    db.execute_sql(
        f"""
         DELETE FROM {cfg ['target']['schema']}.{cfg ['target']['table']}
         WHERE carrier IN ('gas', 'oil')
         AND scenario='eGon2035';
         """
    )

    for carrier in carrier:
        nep = select_nep_power_plants(carrier)

        if nep.empty:
            print(f"DataFrame from NEP for carrier {carrier} is empty!")

        else:
            mastr = select_no_chp_combustion_mastr(carrier)

            # Assign voltage level to MaStR
            mastr["voltage_level"] = assign_voltage_level(
                mastr.rename({"el_capacity": "Nettonennleistung"}, axis=1),
                cfg,
                WORKING_DIR_MASTR_OLD,
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
            )

            # Match plants from NEP list using city and capacity
            matched, mastr, nep = match_nep_no_chp(
                nep,
                mastr,
                matched,
                buffer_capacity=0.1,
                consider_carrier=False,
                consider_location="city",
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
            )

            # Match remaining plants from NEP using the federal state
            matched, mastr, nep = match_nep_no_chp(
                nep,
                mastr,
                matched,
                buffer_capacity=0.1,
                consider_location="federal_state",
                consider_carrier=False,
            )

            # Match remaining plants from NEP using the federal state
            matched, mastr, nep = match_nep_no_chp(
                nep,
                mastr,
                matched,
                buffer_capacity=0.7,
                consider_location="federal_state",
                consider_carrier=False,
            )

            print(f"{matched.el_capacity.sum()} MW of {carrier} matched")
            print(f"{nep.c2035_capacity.sum()} MW of {carrier} not matched")

            matched.crs = "EPSG:4326"

            # Assign bus_id
            # Load grid district polygons
            mv_grid_districts = db.select_geodataframe(
                f"""
            SELECT * FROM {cfg['sources']['egon_mv_grid_district']}
            """,
                epsg=4326,
            )

            ehv_grid_districts = db.select_geodataframe(
                f"""
            SELECT * FROM {cfg['sources']['ehv_voronoi']}
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
    # Get configuration
    cfg = egon.data.config.datasets()["power_plants"]
    boundary = egon.data.config.settings()["egon-data"]["--dataset-boundary"]

    db.execute_sql(
        f"""
        DELETE FROM {cfg['target']['schema']}.{cfg['target']['table']}
        WHERE carrier ='others'
        """
    )

    # Define scenario, carrier 'others' is only present in 'eGon2035'
    scenario = "eGon2035"

    # Select target values for carrier 'others'
    target = db.select_dataframe(
        f"""
        SELECT sum(capacity) as capacity, carrier, scenario_name, nuts
            FROM {cfg['sources']['capacities']}
            WHERE scenario_name = '{scenario}'
            AND carrier = 'others'
            GROUP BY carrier, nuts, scenario_name;
        """
    )

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
    mastr_sludge = pd.read_csv(
        WORKING_DIR_MASTR_OLD / cfg["sources"]["mastr_gsgk"]
    ).query(
        """EinheitBetriebsstatus=='InBetrieb'and Energietraeger=='Klärschlamm'"""  # noqa: E501
    )
    mastr_geothermal = pd.read_csv(
        WORKING_DIR_MASTR_OLD / cfg["sources"]["mastr_gsgk"]
    ).query(
        "EinheitBetriebsstatus=='InBetrieb' and Energietraeger=='Geothermie' "
        "and Technologie == 'ORCOrganicRankineCycleAnlage'"
    )

    mastr_sg = mastr_sludge.append(mastr_geothermal)

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
    mastr_others = mastr_sg.append(mastr_combustion).reset_index()

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
    mastr_prox = assign_bus_id(mastr_prox, cfg)
    mastr_prox = mastr_prox.set_crs(4326, allow_override=True)

    # Insert into target table
    session = sessionmaker(bind=db.engine())()
    for i, row in mastr_prox.iterrows():
        entry = EgonPowerPlants(
            sources=row.el_capacity,
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


def power_plants_status_quo(scn_name="status2019"):
    con = db.engine()
    session = sessionmaker(bind=db.engine())()
    cfg = egon.data.config.datasets()["power_plants"]

    db.execute_sql(
        f"""
        DELETE FROM {cfg['target']['schema']}.{cfg['target']['table']}
        WHERE carrier IN ('wind_onshore', 'solar', 'biomass',
                          'run_of_river', 'reservoir', 'solar_rooftop')
        AND scenario = '{scn_name}'
        """
    )

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
        SELECT * FROM {cfg['sources']['egon_mv_grid_district']}
        """,
        con,
    )
    mv_grid_districts.geom = mv_grid_districts.geom.to_crs(4326)

    def fill_missing_bus_and_geom(gens, carrier):
        # drop generators without data to get geometry.
        drop_id = gens[
            (gens.geom.is_empty) & ~(gens.city.isin(geom_municipalities.index))
        ].index
        new_geom = gens["capacity"][
            (gens.geom.is_empty) & (gens.city.isin(geom_municipalities.index))
        ]
        print(
            f"""{len(drop_id)} {carrier} generator(s) ({gens.loc[drop_id, 'capacity']
              .sum()}MW) were drop"""
        )

        print(
            f"""{len(new_geom)} {carrier} generator(s) ({new_geom
              .sum()}MW) received a geom based on city
              """
        )
        gens.drop(index=drop_id, inplace=True)

        # assign missing geometries based on city and buses based on geom

        gens["geom"] = gens.apply(
            lambda x: geom_municipalities.at[x["city"], "geom"]
            if x["geom"].is_empty
            else x["geom"],
            axis=1,
        )
        gens["bus_id"] = gens.sjoin(
            mv_grid_districts[["bus_id", "geom"]], how="left"
        ).bus_id_right.values

        gens = gens.dropna(subset=["bus_id"])
        # convert geom to WKB
        gens["geom"] = gens["geom"].to_wkt()

        return gens

    # Write hydro power plants in supply.egon_power_plants
    map_hydro = {
        "Laufwasseranlage": "run_of_river",
        "Speicherwasseranlage": "reservoir",
    }

    hydro = gpd.GeoDataFrame.from_postgis(
        f"""SELECT * FROM {cfg['sources']['hydro']}
        WHERE plant_type IN ('Laufwasseranlage', 'Speicherwasseranlage')""",
        con,
        geom_col="geom",
    )

    hydro = fill_missing_bus_and_geom(hydro, carrier="hydro")

    for i, row in hydro.iterrows():
        entry = EgonPowerPlants(
            sources={"el_capacity": "MaStR"},
            source_id={"MastrNummer": row.gens_id},
            carrier=map_hydro[row.plant_type],
            el_capacity=row.capacity,
            voltage_level=row.voltage_level,
            bus_id=row.bus_id,
            scenario=scn_name,
            geom=row.geom,
        )
        session.add(entry)
    session.commit()

    print(
        f"""
          {len(hydro)} hydro generators with a total installed capacity of
          {hydro.capacity.sum()}MW were inserted in db
          """
    )

    # Write biomass power plants in supply.egon_power_plants
    biomass = gpd.GeoDataFrame.from_postgis(
        f"""SELECT * FROM {cfg['sources']['biomass']}""", con, geom_col="geom"
    )

    biomass = fill_missing_bus_and_geom(biomass, carrier="biomass")

    for i, row in biomass.iterrows():
        entry = EgonPowerPlants(
            sources={"el_capacity": "MaStR"},
            source_id={"MastrNummer": row.gens_id},
            carrier="biomass",
            el_capacity=row.capacity,
            scenario=scn_name,
            bus_id=row.bus_id,
            voltage_level=row.voltage_level,
            geom=row.geom,
        )
        session.add(entry)
    session.commit()

    print(
        f"""
          {len(biomass)} biomass generators with a total installed capacity of
          {biomass.capacity.sum()}MW were inserted in db
          """
    )

    # Write solar power plants in supply.egon_power_plants
    solar = gpd.GeoDataFrame.from_postgis(
        f"""SELECT * FROM {cfg['sources']['pv']}
        WHERE site_type IN ('Freifläche',
        'Bauliche Anlagen (Hausdach, Gebäude und Fassade)') """,
        con,
        geom_col="geom",
    )
    map_solar = {
        "Freifläche": "solar",
        "Bauliche Anlagen (Hausdach, Gebäude und Fassade)": "solar_rooftop",
    }
    solar["site_type"] = solar["site_type"].map(map_solar)

    solar = fill_missing_bus_and_geom(solar, carrier="solar")

    solar = pd.DataFrame(solar, index=solar.index)
    for i, row in solar.iterrows():
        entry = EgonPowerPlants(
            sources={"el_capacity": "MaStR"},
            source_id={"MastrNummer": row.gens_id},
            carrier=row.site_type,
            el_capacity=row.capacity,
            scenario=scn_name,
            bus_id=row.bus_id,
            voltage_level=row.voltage_level,
            geom=row.geom,
        )
        session.add(entry)
    session.commit()

    print(
        f"""
          {len(solar)} solar generators with a total installed capacity of
          {solar.capacity.sum()}MW were inserted in db
          """
    )

    # Write wind_onshore power plants in supply.egon_power_plants
    wind_onshore = gpd.GeoDataFrame.from_postgis(
        f"""SELECT * FROM {cfg['sources']['wind']}""", con, geom_col="geom"
    )

    wind_onshore = fill_missing_bus_and_geom(
        wind_onshore, carrier="wind_onshore"
    )

    for i, row in wind_onshore.iterrows():
        entry = EgonPowerPlants(
            sources={"el_capacity": "MaStR"},
            source_id={"MastrNummer": row.gens_id},
            carrier="wind_onshore",
            el_capacity=row.capacity,
            scenario=scn_name,
            bus_id=row.bus_id,
            voltage_level=row.voltage_level,
            geom=row.geom,
        )
        session.add(entry)
    session.commit()

    print(
        f"""
          {len(wind_onshore)} wind_onshore generators with a total installed capacity of
          {wind_onshore.capacity.sum()}MW were inserted in db
          """
    )

    return
