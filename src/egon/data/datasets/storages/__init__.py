"""The central module containing all code dealing with power plant data.
"""
from geoalchemy2 import Geometry
from sqlalchemy import BigInteger, Column, Float, Integer, Sequence, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from egon.data.datasets.storages.pumped_hydro import (
    select_mastr_pumped_hydro,
    select_nep_pumped_hydro,
    match_storage_units,
    get_location,
    apply_voltage_level_thresholds,
)
from egon.data.datasets.power_plants import assign_voltage_level
import geopandas as gpd
import pandas as pd

from egon.data import db, config
from egon.data.datasets import Dataset


Base = declarative_base()


class EgonStorages(Base):
    __tablename__ = "egon_storages"
    __table_args__ = {"schema": "supply"}
    id = Column(BigInteger, Sequence("storage_seq"), primary_key=True)
    sources = Column(JSONB)
    source_id = Column(JSONB)
    carrier = Column(String)
    el_capacity = Column(Float)
    bus_id = Column(Integer)
    voltage_level = Column(Integer)
    scenario = Column(String)
    geom = Column(Geometry("POINT", 4326))


class PumpedHydro(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="Storages",
            version="0.0.1",
            dependencies=dependencies,
            tasks=(create_tables,
                   allocate_pumped_hydro_eGon2035,
                   allocate_pumped_hydro_eGon100RE),
        )


def create_tables():
    """Create tables for power plant data
    Returns
    -------
    None.
    """

    cfg = config.datasets()["storages"]
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {cfg['target']['schema']};")
    engine = db.engine()
    db.execute_sql(
        f"""DROP TABLE IF EXISTS
        {cfg['target']['schema']}.{cfg['target']['table']}"""
    )

    db.execute_sql("""DROP SEQUENCE IF EXISTS pp_seq""")
    EgonStorages.__table__.create(bind=engine, checkfirst=True)


def allocate_pumped_hydro_eGon2035(export=True):
    """Allocates pumped_hydro plants for eGon2035 scenario and either exports
    results to data base or returns as a dataframe

    Parameters
    ----------
    export : bool
        Choose if allocated pumped hydro plants should be exported to the data
        base. The default is True.
        If export=False a data frame will be returned

    Returns
    -------
    power_plants : pandas.DataFrame
        List of pumped hydro plants in 'eGon2035' scenario
    """

    carrier = "pumped_hydro"

    cfg = config.datasets()["power_plants"]

    nep = select_nep_pumped_hydro()
    mastr = select_mastr_pumped_hydro()

    # Assign voltage level to MaStR
    mastr["voltage_level"] = assign_voltage_level(
        mastr.rename({"el_capacity": "Nettonennleistung"}, axis=1), cfg
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

    # Match pumped_hydro units from NEP list
    # using PLZ and capacity
    matched, mastr, nep = match_storage_units(
        nep, mastr, matched, buffer_capacity=0.1, consider_carrier=False
    )

    # Match plants from NEP list using plz,
    # neglecting the capacity
    matched, mastr, nep = match_storage_units(
        nep,
        mastr,
        matched,
        consider_location="plz",
        consider_carrier=False,
        consider_capacity=False,
    )

    # Match plants from NEP list using city,
    # neglecting the capacity
    matched, mastr, nep = match_storage_units(
        nep,
        mastr,
        matched,
        consider_location="city",
        consider_carrier=False,
        consider_capacity=False,
    )

    # Match remaining plants from NEP using the federal state
    matched, mastr, nep = match_storage_units(
        nep,
        mastr,
        matched,
        buffer_capacity=0.1,
        consider_location="federal_state",
        consider_carrier=False,
    )

    # Match remaining plants from NEP using the federal state
    matched, mastr, nep = match_storage_units(
        nep,
        mastr,
        matched,
        buffer_capacity=0.7,
        consider_location="federal_state",
        consider_carrier=False,
    )

    print(f"{matched.el_capacity.sum()} MW of {carrier} matched")
    print(f"{nep.c2035_capacity.sum()} MW of {carrier} not matched")

    if nep.c2035_capacity.sum() > 0:

        # Get location using geolocator and city information
        located, unmatched = get_location(nep)

        # Bring both dataframes together
        matched = matched.append(
            located[
                [
                    "carrier",
                    "el_capacity",
                    "scenario",
                    "geometry",
                    "source",
                    "MaStRNummer",
                ]
            ],
            ignore_index=True,
        )

    # Set CRS
    matched.crs = "EPSG:4326"

    # Assign voltage level
    matched = apply_voltage_level_thresholds(matched)

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

    # Delete existing units in the target table
    db.execute_sql(
        f""" DELETE FROM {cfg ['target']['schema']}.{cfg ['target']['table']}
        WHERE carrier IN ('pumped_hydro')
        AND scenario='eGon2035';"""
    )

    # If export = True export pumped_hydro plants to data base

    if export:
        # Insert into target table
        session = sessionmaker(bind=db.engine())()
        for i, row in power_plants.iterrows():
            entry = EgonStorages(
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

    else:
        return power_plants


def allocate_pumped_hydro_eGon100RE():
    """Allocates pumped_hydro plants for eGon100RE scenario based on a
    prox-to-now method applied on allocated pumped-hydro plants in the eGon2035
    scenario.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    carrier = "pumped_hydro"
    cfg = config.datasets()["power_plants"]
    boundary = config.settings()["egon-data"]["--dataset-boundary"]

    # Select installed capacity for pumped_hydro in eGon100RE scenario from
    # scenario capacities table
    capacity = db.select_dataframe(
        f"""
        SELECT capacity
        FROM {cfg['sources']['capacities']}
        WHERE carrier = '{carrier}'
        AND scenario_name = 'eGon100RE';
        """
    )

    if boundary == "Schleswig-Holstein":
        # Break capacity of pumped hydron plants down SH share in eGon2035
        capacity_phes = capacity.iat[0,0]*0.0176

    elif boundary == "Everything":
        # Select national capacity for pumped hydro
        capacity_phes = capacity.iat[0,0]

    else:
        raise ValueError(f"'{boundary}' is not a valid dataset boundary.")

    # Get allocation of pumped_hydro plants in eGon2035 scenario as the reference
    # for the distribution in eGon100RE scenario
    allocation = allocate_pumped_hydro_eGon2035(export=False)

    scaling_factor = capacity_phes/allocation.el_capacity.sum()

    power_plants = allocation.copy()
    power_plants["scenario"] = "eGon100RE"
    power_plants["el_capacity"] = allocation.el_capacity*scaling_factor

    # Insert into target table
    session = sessionmaker(bind=db.engine())()
    for i, row in power_plants.iterrows():
        entry = EgonStorages(
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
