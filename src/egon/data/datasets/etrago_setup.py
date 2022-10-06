# coding: utf-8
from geoalchemy2.types import Geometry
from shapely.geometry import LineString
from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    Numeric,
    String,
    Text,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset

Base = declarative_base()
metadata = Base.metadata


class EtragoSetup(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="EtragoSetup",
            version="0.0.10",
            dependencies=dependencies,
            tasks=(create_tables, {temp_resolution, insert_carriers}),
        )


class EgonPfHvBus(Base):
    __tablename__ = "egon_etrago_bus"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    bus_id = Column(BigInteger, primary_key=True, nullable=False)
    v_nom = Column(Float(53), server_default="1.")
    type = Column(Text)
    carrier = Column(Text)
    v_mag_pu_set = Column(Float(53))
    v_mag_pu_min = Column(Float(53), server_default="0.")
    v_mag_pu_max = Column(Float(53), server_default="inf")
    x = Column(Float(53), server_default="0.")
    y = Column(Float(53), server_default="0.")
    geom = Column(Geometry("POINT", 4326), index=True)
    country = Column(Text, server_default=text("'DE'::text"))


class EgonPfHvBusTimeseries(Base):
    __tablename__ = "egon_etrago_bus_timeseries"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    bus_id = Column(BigInteger, primary_key=True, nullable=False)
    v_mag_pu_set = Column(ARRAY(Float(precision=53)))


class EgonPfHvGenerator(Base):
    __tablename__ = "egon_etrago_generator"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    generator_id = Column(BigInteger, primary_key=True, nullable=False)
    bus = Column(BigInteger)
    control = Column(Text)
    type = Column(Text)
    carrier = Column(Text)
    p_nom = Column(Float(53), server_default="0.")
    p_nom_extendable = Column(Boolean, server_default="False")
    p_nom_min = Column(Float(53), server_default="0.")
    p_nom_max = Column(Float(53), server_default="inf")
    p_min_pu = Column(Float(53), server_default="0.")
    p_max_pu = Column(Float(53), server_default="1.")
    p_set = Column(Float(53))
    q_set = Column(Float(53))
    sign = Column(Float(53), server_default="1.")
    marginal_cost = Column(Float(53), server_default="0.")
    build_year = Column(BigInteger, server_default="0")
    lifetime = Column(Float(53), server_default="inf")
    capital_cost = Column(Float(53), server_default="0.")
    efficiency = Column(Float(53), server_default="1.")
    committable = Column(Boolean, server_default="False")
    start_up_cost = Column(Float(53), server_default="0.")
    shut_down_cost = Column(Float(53), server_default="0.")
    min_up_time = Column(BigInteger, server_default="0")
    min_down_time = Column(BigInteger, server_default="0")
    up_time_before = Column(BigInteger, server_default="0")
    down_time_before = Column(BigInteger, server_default="0")
    ramp_limit_up = Column(Float(53), server_default="NaN")
    ramp_limit_down = Column(Float(53), server_default="NaN")
    ramp_limit_start_up = Column(Float(53), server_default="1.")
    ramp_limit_shut_down = Column(Float(53), server_default="1.")
    e_nom_max = Column(
        Float(53), server_default="inf"
    )  # [MWh(/y)] Value to be used in eTraGo to set constraint for the production over the year


class EgonPfHvGeneratorTimeseries(Base):
    __tablename__ = "egon_etrago_generator_timeseries"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    generator_id = Column(Integer, primary_key=True, nullable=False)
    temp_id = Column(Integer, primary_key=True, nullable=False)
    p_set = Column(ARRAY(Float(precision=53)))
    q_set = Column(ARRAY(Float(precision=53)))
    p_min_pu = Column(ARRAY(Float(precision=53)))
    p_max_pu = Column(ARRAY(Float(precision=53)))
    marginal_cost = Column(ARRAY(Float(precision=53)))


class EgonPfHvLine(Base):
    __tablename__ = "egon_etrago_line"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    line_id = Column(BigInteger, primary_key=True, nullable=False)
    bus0 = Column(BigInteger)
    bus1 = Column(BigInteger)
    type = Column(Text)
    carrier = Column(Text)
    x = Column(Numeric, server_default="0.")
    r = Column(Numeric, server_default="0.")
    g = Column(Numeric, server_default="0.")
    b = Column(Numeric, server_default="0.")
    s_nom = Column(Numeric, server_default="0.")
    s_nom_extendable = Column(Boolean, server_default="False")
    s_nom_min = Column(Float(53), server_default="0.")
    s_nom_max = Column(Float(53), server_default="inf")
    s_max_pu = Column(Float(53), server_default="1.")
    build_year = Column(BigInteger, server_default="0")
    lifetime = Column(Float(53), server_default="inf")
    capital_cost = Column(Float(53), server_default="0.")
    length = Column(Float(53), server_default="0.")
    cables = Column(Integer)
    terrain_factor = Column(Float(53), server_default="1.")
    num_parallel = Column(Float(53), server_default="1.")
    v_ang_min = Column(Float(53), server_default="-inf")
    v_ang_max = Column(Float(53), server_default="inf")
    v_nom = Column(Float(53))
    geom = Column(Geometry("MULTILINESTRING", 4326))
    topo = Column(Geometry("LINESTRING", 4326))


class EgonPfHvLineTimeseries(Base):
    __tablename__ = "egon_etrago_line_timeseries"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    line_id = Column(BigInteger, primary_key=True, nullable=False)
    temp_id = Column(Integer, primary_key=True, nullable=False)
    s_max_pu = Column(ARRAY(Float(precision=53)))


class EgonPfHvLink(Base):
    __tablename__ = "egon_etrago_link"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    link_id = Column(BigInteger, primary_key=True, nullable=False)
    bus0 = Column(BigInteger)
    bus1 = Column(BigInteger)
    type = Column(Text)
    carrier = Column(Text)
    efficiency = Column(Float(53), server_default="1.")
    build_year = Column(BigInteger, server_default="0")
    lifetime = Column(Float(53), server_default="inf")
    p_nom = Column(Numeric, server_default="0.")
    p_nom_extendable = Column(Boolean, server_default="False")
    p_nom_min = Column(Float(53), server_default="0.")
    p_nom_max = Column(Float(53), server_default="inf")
    p_min_pu = Column(Float(53), server_default="0.")
    p_max_pu = Column(Float(53), server_default="1.")
    p_set = Column(Float(53))
    capital_cost = Column(Float(53), server_default="0.")
    marginal_cost = Column(Float(53), server_default="0.")
    length = Column(Float(53), server_default="0.")
    terrain_factor = Column(Float(53), server_default="1.")
    geom = Column(Geometry("MULTILINESTRING", 4326))
    topo = Column(Geometry("LINESTRING", 4326))


class EgonPfHvLinkTimeseries(Base):
    __tablename__ = "egon_etrago_link_timeseries"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    link_id = Column(BigInteger, primary_key=True, nullable=False)
    temp_id = Column(Integer, primary_key=True, nullable=False)
    p_set = Column(ARRAY(Float(precision=53)))
    p_min_pu = Column(ARRAY(Float(precision=53)))
    p_max_pu = Column(ARRAY(Float(precision=53)))
    efficiency = Column(ARRAY(Float(precision=53)))
    marginal_cost = Column(ARRAY(Float(precision=53)))


class EgonPfHvLoad(Base):
    __tablename__ = "egon_etrago_load"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    load_id = Column(BigInteger, primary_key=True, nullable=False)
    bus = Column(BigInteger)
    type = Column(Text)
    carrier = Column(Text)
    p_set = Column(Float(53))
    q_set = Column(Float(53))
    sign = Column(Float(53), server_default="-1.")


class EgonPfHvLoadTimeseries(Base):
    __tablename__ = "egon_etrago_load_timeseries"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    load_id = Column(BigInteger, primary_key=True, nullable=False)
    temp_id = Column(Integer, primary_key=True, nullable=False)
    p_set = Column(ARRAY(Float(precision=53)))
    q_set = Column(ARRAY(Float(precision=53)))


class EgonPfHvCarrier(Base):
    __tablename__ = "egon_etrago_carrier"
    __table_args__ = {"schema": "grid"}

    name = Column(Text, primary_key=True, nullable=False)
    co2_emissions = Column(Float(53), server_default="0.")
    color = Column(Text)
    nice_name = Column(Text)
    commentary = Column(Text)


class EgonPfHvStorage(Base):
    __tablename__ = "egon_etrago_storage"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    storage_id = Column(BigInteger, primary_key=True, nullable=False)
    bus = Column(BigInteger)
    control = Column(Text)
    type = Column(Text)
    carrier = Column(Text)
    p_nom = Column(Float(53), server_default="0.")
    p_nom_extendable = Column((Boolean), server_default="False")
    p_nom_min = Column(Float(53), server_default="0.")
    p_nom_max = Column(Float(53), server_default="inf")
    p_min_pu = Column(Float(53), server_default="-1.")
    p_max_pu = Column(Float(53), server_default="1.")
    p_set = Column(Float(53))
    q_set = Column(Float(53))
    sign = Column(Float(53), server_default="1")
    marginal_cost = Column(Float(53), server_default="0.")
    capital_cost = Column(Float(53), server_default="0.")
    build_year = Column(BigInteger, server_default="0")
    lifetime = Column(Float(53), server_default="inf")
    state_of_charge_initial = Column(Float(53), server_default="0")
    cyclic_state_of_charge = Column(Boolean, server_default="False")
    state_of_charge_set = Column(Float(53))
    max_hours = Column(Float(53), server_default="1")
    efficiency_store = Column(Float(53), server_default="1.")
    efficiency_dispatch = Column(Float(53), server_default="1.")
    standing_loss = Column(Float(53), server_default="0.")
    inflow = Column(Float(53), server_default="0.")


class EgonPfHvStorageTimeseries(Base):
    __tablename__ = "egon_etrago_storage_timeseries"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    storage_id = Column(BigInteger, primary_key=True, nullable=False)
    temp_id = Column(Integer, primary_key=True, nullable=False)
    p_set = Column(ARRAY(Float(precision=53)))
    q_set = Column(ARRAY(Float(precision=53)))
    p_min_pu = Column(ARRAY(Float(precision=53)))
    p_max_pu = Column(ARRAY(Float(precision=53)))
    state_of_charge_set = Column(ARRAY(Float(precision=53)))
    inflow = Column(ARRAY(Float(precision=53)))
    marginal_cost = Column(ARRAY(Float(precision=53)))


class EgonPfHvStore(Base):
    __tablename__ = "egon_etrago_store"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    store_id = Column(BigInteger, primary_key=True, nullable=False)
    bus = Column(BigInteger)
    type = Column(Text)
    carrier = Column(Text)
    e_nom = Column(Float(53), server_default="0.")
    e_nom_extendable = Column((Boolean), server_default="False")
    e_nom_min = Column(Float(53), server_default="0.")
    e_nom_max = Column(Float(53), server_default="inf")
    e_min_pu = Column(Float(53), server_default="0.")
    e_max_pu = Column(Float(53), server_default="1.")
    p_set = Column(Float(53))
    q_set = Column(Float(53))
    e_initial = Column(Float(53), server_default="0.")
    e_cyclic = Column(Boolean, server_default="False")
    sign = Column(Float(53), server_default="1")
    marginal_cost = Column(Float(53), server_default="0.")
    capital_cost = Column(Float(53), server_default="0.")
    standing_loss = Column(Float(53), server_default="0.")
    build_year = Column(BigInteger, server_default="0")
    lifetime = Column(Float(53), server_default="inf")


class EgonPfHvStoreTimeseries(Base):
    __tablename__ = "egon_etrago_store_timeseries"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    store_id = Column(BigInteger, primary_key=True, nullable=False)
    temp_id = Column(Integer, primary_key=True, nullable=False)
    p_set = Column(ARRAY(Float(precision=53)))
    q_set = Column(ARRAY(Float(precision=53)))
    e_min_pu = Column(ARRAY(Float(precision=53)))
    e_max_pu = Column(ARRAY(Float(precision=53)))
    marginal_cost = Column(ARRAY(Float(precision=53)))


class EgonPfHvTempResolution(Base):
    __tablename__ = "egon_etrago_temp_resolution"
    __table_args__ = {"schema": "grid"}

    temp_id = Column(BigInteger, primary_key=True, nullable=False)
    timesteps = Column(BigInteger, nullable=False)
    resolution = Column(Text)
    start_time = Column(DateTime)


class EgonPfHvTransformer(Base):
    __tablename__ = "egon_etrago_transformer"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    trafo_id = Column(BigInteger, primary_key=True, nullable=False)
    bus0 = Column(BigInteger)
    bus1 = Column(BigInteger)
    type = Column(Text)
    model = Column((Text), server_default="t")
    x = Column((Numeric), server_default="0.")
    r = Column((Numeric), server_default="0.")
    g = Column((Numeric), server_default="0.")
    b = Column((Numeric), server_default="0.")
    s_nom = Column(Float(53), server_default="0.")
    s_nom_extendable = Column((Boolean), server_default="False")
    s_nom_min = Column(Float(53), server_default="0.")
    s_nom_max = Column(Float(53), server_default="inf")
    s_max_pu = Column(Float(53), server_default="1.")
    tap_ratio = Column(Float(53), server_default="1.")
    tap_side = Column((BigInteger), server_default="0")
    tap_position = Column((BigInteger), server_default="0")
    phase_shift = Column(Float(53), server_default="0.")
    build_year = Column(BigInteger, server_default="0")
    lifetime = Column(Float(53), server_default="inf")
    v_ang_min = Column(Float(53), server_default="-inf")
    v_ang_max = Column(Float(53), server_default="inf")
    capital_cost = Column(Float(53), server_default="0.")
    num_parallel = Column(Float(53), server_default="1.")
    geom = Column(Geometry("MULTILINESTRING", 4326))
    topo = Column(Geometry("LINESTRING", 4326))


class EgonPfHvTransformerTimeseries(Base):
    __tablename__ = "egon_etrago_transformer_timeseries"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(String, primary_key=True, nullable=False)
    trafo_id = Column(BigInteger, primary_key=True, nullable=False)
    temp_id = Column(Integer, primary_key=True, nullable=False)
    s_max_pu = Column(ARRAY(Float(precision=53)))


class EgonPfHvBusmap(Base):
    __tablename__ = "egon_etrago_hv_busmap"
    __table_args__ = {"schema": "grid"}

    scn_name = Column(Text, primary_key=True, nullable=False)
    bus0 = Column(Text, primary_key=True, nullable=False)
    bus1 = Column(Text, primary_key=True, nullable=False)
    path_length = Column(Numeric)
    version = Column(Text, primary_key=True, nullable=False)


def create_tables():
    """Create tables for eTraGo input data.
    Returns
    -------
    None.
    """
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS grid;")
    engine = db.engine()

    ##################### drop tables with old names #########################
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_bus;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_bus_timeseries;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_carrier;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_generator;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_generator_timeseries;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_line;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_line_timeseries;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_link;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_link_timeseries;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_load;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_load_timeseries;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_storage;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_storage_timeseries;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_store;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_store_timeseries;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_temp_resolution;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_transformer;"""
    )
    db.execute_sql(
        """
        DROP TABLE IF EXISTS grid.egon_pf_hv_transformer_timeseries;"""
    )
    ##########################################################################

    # Drop existing tables
    EgonPfHvBus.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvBusTimeseries.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvGenerator.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvGeneratorTimeseries.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvLine.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvLineTimeseries.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvLink.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvLinkTimeseries.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvLoad.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvLoadTimeseries.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvCarrier.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvStorage.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvStorageTimeseries.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvStore.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvStoreTimeseries.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvTempResolution.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvTransformer.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvTransformerTimeseries.__table__.drop(bind=engine, checkfirst=True)
    EgonPfHvBusmap.__table__.drop(bind=engine, checkfirst=True)
    # Create new tables
    EgonPfHvBus.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvBusTimeseries.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvGenerator.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvGeneratorTimeseries.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvLine.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvLineTimeseries.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvLink.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvLinkTimeseries.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvLoad.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvLoadTimeseries.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvCarrier.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvStorage.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvStorageTimeseries.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvStore.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvStoreTimeseries.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvTempResolution.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvTransformer.__table__.create(bind=engine, checkfirst=True)
    EgonPfHvTransformerTimeseries.__table__.create(
        bind=engine, checkfirst=True
    )
    EgonPfHvBusmap.__table__.create(bind=engine, checkfirst=True)


def temp_resolution():
    """Insert temporal resolution for etrago

    Returns
    -------
    None.

    """

    db.execute_sql(
        """
        INSERT INTO grid.egon_etrago_temp_resolution
        (temp_id, timesteps, resolution, start_time)
        SELECT 1, 8760, 'h', TIMESTAMP '2011-01-01 00:00:00';
        """
    )


def insert_carriers():
    """Insert list of carriers into eTraGo table

    Returns
    -------
    None.

    """
    # Delete existing entries
    db.execute_sql(
        """
        DELETE FROM grid.egon_etrago_carrier
        """
    )

    # List carrier names from all components
    df = pd.DataFrame(
        data={
            "name": [
                "biogas",
                "biogas_feedin",
                "biogas_to_gas",
                "biomass",
                "pv",
                "wind_offshore",
                "wind_onshore",
                "central_heat_pump",
                "central_resistive_heater",
                "CH4",
                "CH4_for_industry",
                "CH4_system_boundary",
                "CH4_to_H2",
                "dsm",
                "H2",
                "H2_feedin",
                "H2_for_industry",
                "H2_grid",
                "H2_gridextension",
                "H2_hgv_load",
                "H2_overground",
                "H2_retrofit",
                "H2_saltcavern",
                "H2_system_boundary",
                "H2_to_CH4",
                "H2_to_power",
                "H2_underground",
                "rural_heat_pump",
                "industrial_biomass_CHP",
                "industrial_gas_CHP",
                "central_biomass_CHP_heat",
                "central_biomass_CHP",
                "central_gas_CHP",
                "central_gas_CHP_heat",
                "power_to_H2",
                "rural_gas_boiler",
                "central_gas_boiler",
                "solar_thermal_collector",
                "geo_thermal",
                "AC",
                "central_heat",
                "rural_heat",
                "natural_gas_feedin",
                "pumped_hydro",
                "battery",
                "OCGT",
            ]
        }
    )

    # Insert data into database
    df.to_sql(
        "egon_etrago_carrier",
        schema="grid",
        con=db.engine(),
        if_exists="append",
        index=False,
    )


def check_carriers():
    """Check if any eTraGo table has carriers not included in the carrier table.

    Raises
    ------
    ValueError if carriers that are not defined in the carriers table are
    used in any eTraGo table.
    """
    carriers = db.select_dataframe(
        f"""
        SELECT name FROM grid.egon_etrago_carrier
        """
    )
    unknown_carriers = {}
    tables = ["bus", "store", "storage", "link", "line", "generator", "load"]

    for table in tables:
        # Delete existing entries
        data = db.select_dataframe(
            f"""
            SELECT carrier FROM grid.egon_etrago_{table}
            """
        )
        unknown_carriers[table] = data[~data["carrier"].isin(carriers)][
            "carrier"
        ].unique()

    if len(unknown_carriers) > 0:
        msg = (
            "The eTraGo tables contain carriers, that are not included in the "
            "carrier table:\n"
        )
        for table, carriers in unknown_carriers.items():
            carriers = [str(c) for c in carriers]
            if len(carriers) > 0:
                msg += table + ": '" + "', '".join(carriers) + "'\n"

        raise ValueError(msg)


def link_geom_from_buses(df, scn_name):
    """Add LineString geometry accoring to geometry of buses to links

    Parameters
    ----------
    df : pandas.DataFrame
        List of eTraGo links with bus0 and bus1 but without topology
    scn_name : str
        Scenario name

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        List of eTraGo links with bus0 and bus1 but with topology

    """

    geom_buses = db.select_geodataframe(
        f"""
        SELECT bus_id, geom
        FROM grid.egon_etrago_bus
        WHERE scn_name = '{scn_name}'
        """,
        index_col="bus_id",
        epsg=4326,
    )

    # Create geometry columns for bus0 and bus1
    df["geom_0"] = geom_buses.geom[df.bus0.values].values
    df["geom_1"] = geom_buses.geom[df.bus1.values].values

    geometry = df.apply(
        lambda x: LineString([x["geom_0"], x["geom_1"]]), axis=1
    )
    df = df.drop(["geom_0", "geom_1"], axis=1)

    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=4326).rename_geometry(
        "topo"
    )

    return gdf
