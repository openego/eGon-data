"""The central module containing all code dealing with power plant data.
"""

from pathlib import Path

from geoalchemy2 import Geometry
from sqlalchemy import BigInteger, Column, Float, Integer, Sequence, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets import Dataset
from egon.data.datasets.mastr import (
    WORKING_DIR_MASTR_NEW,
    WORKING_DIR_MASTR_OLD,
)
from egon.data.datasets.electrical_neighbours import entsoe_to_bus_etrago
from egon.data.datasets.mv_grid_districts import Vg250GemClean
from egon.data.datasets.power_plants import assign_bus_id, assign_voltage_level
from egon.data.datasets.storages.home_batteries import (
    allocate_home_batteries_to_buildings,
)
from egon.data.datasets.storages.pumped_hydro import (
    apply_voltage_level_thresholds,
    get_location,
    match_storage_units,
    select_mastr_pumped_hydro,
    select_nep_pumped_hydro,
)
from egon.data.db import session_scope

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


class Storages(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="Storages",
            version="0.0.6",
            dependencies=dependencies,
            tasks=(
                create_tables,
                allocate_pumped_hydro_scn,
                allocate_pv_home_batteries_to_grids,
                # allocate_home_batteries_to_buildings,
            ),
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


def allocate_pumped_hydro(scn, export=True):
    """Allocates pumped_hydro plants for eGon2035 and scenario2019 scenarios
    and either exports results to data base or returns as a dataframe

    Parameters
    ----------
    export : bool
        Choose if allocated pumped hydro plants should be exported to the data
        base. The default is True.
        If export=False a data frame will be returned

    Returns
    -------
    power_plants : pandas.DataFrame
        List of pumped hydro plants in 'eGon2035' and 'scenario2019' scenarios
    """

    carrier = "pumped_hydro"

    cfg = config.datasets()["power_plants"]

    nep = select_nep_pumped_hydro(scn=scn)
    mastr = select_mastr_pumped_hydro()

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

    # Match pumped_hydro units from NEP list
    # using PLZ and capacity
    matched, mastr, nep = match_storage_units(
        nep,
        mastr,
        matched,
        buffer_capacity=0.1,
        consider_carrier=False,
        scn=scn,
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
        scn=scn,
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
        scn=scn,
    )

    # Match remaining plants from NEP using the federal state
    matched, mastr, nep = match_storage_units(
        nep,
        mastr,
        matched,
        buffer_capacity=0.1,
        consider_location="federal_state",
        consider_carrier=False,
        scn=scn,
    )

    # Match remaining plants from NEP using the federal state
    matched, mastr, nep = match_storage_units(
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

    if nep.elec_capacity.sum() > 0:
        # Get location using geolocator and city information
        located, unmatched = get_location(nep)

        # Bring both dataframes together
        matched = pd.concat(
            [
                matched,
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
        f""" DELETE FROM {cfg ['sources']['storages']}
        WHERE carrier IN ('pumped_hydro')
        AND scenario='{scn}';"""
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


def allocate_pumped_hydro_sq(scn_name):
    """
    Allocate pumped hydro by mastr data only. Capacities outside
    germany are assigned to foreign buses. Mastr dump 2024 is used.
    No filter for commissioning is applied.
    Parameters
    ----------
    scn_name

    Returns
    -------

    """
    sources = config.datasets()["power_plants"]["sources"]
    scenario_date_max = scn_name[-4:] + "-12-31 23:59:00"

    # Read-in data from MaStR
    mastr_ph = pd.read_csv(
        WORKING_DIR_MASTR_NEW / sources["mastr_storage"],
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
            "DatumEndgueltigeStilllegung"
        ],
        dtype={"Postleitzahl": str},
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
            "DatumEndgueltigeStilllegung": "decommissioning_date"
        }
    )

    # Select only pumped hydro units
    mastr_ph = mastr_ph.loc[mastr_ph.carrier == "Pumpspeicher"]

    # Select only pumped hydro units which are in operation
    mastr_ph.loc[
        mastr_ph["decommissioning_date"] > scenario_date_max,
        "EinheitBetriebsstatus",
    ] = "InBetrieb"
    mastr_ph = mastr_ph.loc[mastr_ph.EinheitBetriebsstatus == "InBetrieb"]

    # Calculate power in MW
    mastr_ph.loc[:, "el_capacity"] *= 1e-3

    # Create geodataframe from long, lat
    mastr_ph = gpd.GeoDataFrame(
        mastr_ph,
        geometry=gpd.points_from_xy(
            mastr_ph["Laengengrad"], mastr_ph["Breitengrad"]
        ),
        crs="4326",
    )

    # Identify pp without geocord
    mastr_ph_nogeo = mastr_ph.loc[mastr_ph["Laengengrad"].isna()]

    # Remove all PP without geocord (PP<= 30kW)
    mastr_ph = mastr_ph.dropna(subset="Laengengrad")

    # Get geometry of villages/cities with same name of pp with missing geocord
    with session_scope() as session:
        query = session.query(
            Vg250GemClean.gen, Vg250GemClean.geometry
        ).filter(Vg250GemClean.gen.in_(mastr_ph_nogeo.loc[:, "city"].unique()))
        df_cities = gpd.read_postgis(
            query.statement,
            query.session.bind,
            geom_col="geometry",
            crs="4326",
        )

    # Just take the first entry, inaccuracy is negligible as centroid is taken afterwards
    df_cities = df_cities.drop_duplicates("gen", keep="first")

    # Use the centroid instead of polygon of region
    df_cities.loc[:, "geometry"] = df_cities["geometry"].centroid

    # Add centroid geometry to pp without geometry
    mastr_ph_nogeo = pd.merge(
        left=df_cities,
        right=mastr_ph_nogeo,
        right_on="city",
        left_on="gen",
        how="inner",
    ).drop("gen", axis=1)

    mastr_ph = pd.concat([mastr_ph, mastr_ph_nogeo], axis=0)

    # aggregate capacity per location
    agg_cap = mastr_ph.groupby("geometry")["el_capacity"].sum()

    # list mastr number by location
    agg_mastr = mastr_ph.groupby("geometry")["EinheitMastrNummer"].apply(list)

    # remove duplicates by location
    mastr_ph = mastr_ph.drop_duplicates(subset="geometry", keep="first").drop(
        ["el_capacity", "EinheitMastrNummer"], axis=1
    )

    # Adjust capacity
    mastr_ph = pd.merge(
        left=mastr_ph, right=agg_cap, left_on="geometry", right_on="geometry"
    )

    # Adjust capacity
    mastr_ph = pd.merge(
        left=mastr_ph, right=agg_mastr, left_on="geometry", right_on="geometry"
    )

    # Drop small pp <= 03 kW
    mastr_ph = mastr_ph.loc[mastr_ph["el_capacity"] > 30]

    # Apply voltage level by capacity
    mastr_ph = apply_voltage_level_thresholds(mastr_ph)
    mastr_ph["voltage_level"] = mastr_ph["voltage_level"].astype(int)

    # Capacity located outside germany -> will be assigned to foreign buses
    mastr_ph_foreign = mastr_ph.loc[mastr_ph["federal_state"].isna()]

    # Keep only capacities within germany
    mastr_ph = mastr_ph.dropna(subset="federal_state")

    # Asign buses within germany
    mastr_ph = assign_bus_id(mastr_ph, cfg=config.datasets()["power_plants"])
    mastr_ph["bus_id"] = mastr_ph["bus_id"].astype(int)

    # Get foreign central buses
    sql = f"""
    SELECT * FROM grid.egon_etrago_bus
    WHERE scn_name = '{scn_name}'
    and country != 'DE'
    """
    df_foreign_buses = db.select_geodataframe(
        sql, geom_col="geom", epsg="4326"
    )
    central_bus = entsoe_to_bus_etrago(scn_name).to_frame()
    central_bus["geom"] = (
        df_foreign_buses.set_index("bus_id").loc[central_bus[0], "geom"].values
    )
    df_foreign_buses = df_foreign_buses[
        df_foreign_buses["geom"].isin(central_bus["geom"])
    ]
    
    # Assign closest bus at voltage level to foreign pp
    nearest_neighbors = []
    for vl, v_nom in {1: 380, 2: 220, 3: 110}.items():
        ph = mastr_ph_foreign.loc[mastr_ph_foreign["voltage_level"] == vl]
        if ph.empty:
            continue
        bus = df_foreign_buses.loc[
            df_foreign_buses["v_nom"] == v_nom,
            ["v_nom", "country", "bus_id", "geom"],
        ]
        results = gpd.sjoin_nearest(
            left_df=ph, right_df=bus, how="left", distance_col="distance"
        )
        nearest_neighbors.append(results)
    mastr_ph_foreign = pd.concat(nearest_neighbors)

    # Merge foreign pp
    mastr_ph = pd.concat([mastr_ph, mastr_ph_foreign])

    # Reduce to necessary columns
    mastr_ph = mastr_ph[
        [
            "el_capacity",
            "voltage_level",
            "bus_id",
            "geometry",
            "EinheitMastrNummer",
        ]
    ]

    # Rename and format columns
    mastr_ph["carrier"] = "pumped_hydro"
    mastr_ph = mastr_ph.rename(
        columns={"EinheitMastrNummer": "source_id", "geometry": "geom"}
    )
    mastr_ph["source_id"] = mastr_ph["source_id"].apply(
        lambda x: {"MastrNummer": ", ".join(x)}
    )
    mastr_ph = mastr_ph.set_geometry("geom")
    mastr_ph["geom"] = mastr_ph["geom"].apply(lambda x: x.wkb_hex)
    mastr_ph["scenario"] = scn_name
    mastr_ph["sources"] = [
        {"el_capacity": "MaStR aggregated by location"}
    ] * mastr_ph.shape[0]

    # Delete existing units in the target table
    db.execute_sql(
        f""" DELETE FROM supply.egon_storages
        WHERE carrier = 'pumped_hydro'
        AND scenario= '{scn_name}';"""
    )

    with db.session_scope() as session:
        session.bulk_insert_mappings(
            EgonStorages,
            mastr_ph.to_dict(orient="records"),
        )


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
        capacity_phes = capacity.iat[0, 0] * 0.0176

    elif boundary == "Everything":
        # Select national capacity for pumped hydro
        capacity_phes = capacity.iat[0, 0]

    else:
        raise ValueError(f"'{boundary}' is not a valid dataset boundary.")

    # Get allocation of pumped_hydro plants in eGon2035 scenario as the
    # reference for the distribution in eGon100RE scenario
    allocation = allocate_pumped_hydro(scn="status2019", export=False)

    scaling_factor = capacity_phes / allocation.el_capacity.sum()

    power_plants = allocation.copy()
    power_plants["scenario"] = "eGon100RE"
    power_plants["el_capacity"] = allocation.el_capacity * scaling_factor

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


def home_batteries_per_scenario(scenario):
    """Allocates home batteries which define a lower boundary for extendable
    battery storage units. The overall installed capacity is taken from NEP
    for eGon2035 scenario. The spatial distribution of installed battery
    capacities is based on the installed pv rooftop capacity.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    cfg = config.datasets()["storages"]
    dataset = config.settings()["egon-data"]["--dataset-boundary"]

    if scenario == "eGon2035":
        target_file = (
            Path(".")
            / "data_bundle_egon_data"
            / "nep2035_version2021"
            / cfg["sources"]["nep_capacities"]
        )

        capacities_nep = pd.read_excel(
            target_file,
            sheet_name="1.Entwurf_NEP2035_V2021",
            index_col="Unnamed: 0",
        )

        # Select target value in MW
        target = capacities_nep.Summe["PV-Batteriespeicher"] * 1000

    else:
        target = db.select_dataframe(
            f"""
            SELECT capacity
            FROM {cfg['sources']['capacities']}
            WHERE scenario_name = '{scenario}'
            AND carrier = 'battery';
            """
        ).capacity[0]

    pv_rooftop = db.select_dataframe(
        f"""
        SELECT bus, p_nom, generator_id
        FROM {cfg['sources']['generators']}
        WHERE scn_name = '{scenario}'
        AND carrier = 'solar_rooftop'
        AND bus IN
            (SELECT bus_id FROM {cfg['sources']['bus']}
               WHERE scn_name = '{scenario}' AND country = 'DE' );
        """
    )

    if dataset == "Schleswig-Holstein":
        target = target / 16

    battery = pv_rooftop
    battery["p_nom_min"] = target * battery["p_nom"] / battery["p_nom"].sum()
    battery = battery.drop(columns=["p_nom"])

    battery["carrier"] = "home_battery"
    battery["scenario"] = scenario

    if (scenario == "eGon2035") | (scenario == "status2019"):
        source = "NEP"

    else:
        source = "p-e-s"

    battery[
        "source"
    ] = f"{source} capacity allocated based in installed PV rooftop capacity"

    # Insert into target table
    session = sessionmaker(bind=db.engine())()
    for i, row in battery.iterrows():
        entry = EgonStorages(
            sources={"el_capacity": row.source},
            source_id={"generator_id": row.generator_id},
            carrier=row.carrier,
            el_capacity=row.p_nom_min,
            bus_id=row.bus,
            scenario=row.scenario,
        )
        session.add(entry)
    session.commit()


def allocate_pv_home_batteries_to_grids():
    for scn in config.settings()["egon-data"]["--scenarios"]:
        home_batteries_per_scenario(scn)


def allocate_pumped_hydro_scn():
    if "eGon2035" in config.settings()["egon-data"]["--scenarios"]:
        allocate_pumped_hydro(scn="eGon2035")

    if "status2019" in config.settings()["egon-data"]["--scenarios"]:
        allocate_pumped_hydro_sq(scn_name="status2019")

    if "eGon100RE" in config.settings()["egon-data"]["--scenarios"]:
        allocate_pumped_hydro_eGon100RE()
