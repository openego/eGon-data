"""The central module containing all code dealing with power plant data."""

from pathlib import Path

from geoalchemy2 import Geometry
from loguru import logger
from sqlalchemy import BigInteger, Column, Float, Integer, Sequence, String, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.electrical_neighbours import entsoe_to_bus_etrago
from egon.data.datasets.mv_grid_districts import Vg250GemClean
from egon.data.datasets.power_plants import (
    assign_bus_id,
    assign_voltage_level,
    filter_mastr_geometry,
)
from egon.data.datasets.power_plants.pv_rooftop_buildings import (
    SCENARIO_TIMESTAMP,
    determine_end_of_life_gens,
)
from egon.data.datasets.scenario_parameters import get_sector_parameters
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
    commissioning_date = Column(DateTime)
    scenario = Column(String)
    geom = Column(Geometry("POINT", 4326))


class Storages(Dataset):

    sources = DatasetSources(
        files={
            "mastr_storage": "./bnetza_mastr/dump_2025-02-09/bnetza_mastr_storage_cleaned.csv",
            "nep_capacities": "NEP2035_V2021_scnC2035.xlsx",
            "mastr_location": "location_elec_generation_raw.csv",
        },
        tables={
            "capacities": "supply.egon_scenario_capacities",
            "generators": "grid.egon_etrago_generator",
            "bus": "grid.egon_etrago_bus",
            "egon_mv_grid_district": "grid.egon_mv_grid_district",
            "ehv_voronoi": "grid.egon_ehv_substation_voronoi",
            # Added for pumped_hydro.py
            "nep_conv": "supply.egon_nep_conventional_powerplants",
            # Added for home_batteries.py
            "etrago_storage": "grid.egon_etrago_storage",
        },
    )
    targets = DatasetTargets(
        tables={
            "storages": "supply.egon_storages",
            # Added for home_batteries.py
            "home_batteries": "supply.egon_home_batteries",
        }
    )

    """
    Allocates storage units such as pumped hydro and home batteries

    This data set creates interim tables to store information on storage units.
    In addition the target value for the installed capacity of pumped hydro
    storage units are spatially allocated using information of existing plants
    from the official registry Markstammdatenregister. After allocating the
    plants missing information such as the voltage level and the correct grid
    connection point are added.
    This data set also allocates the target value of home batteries spatially
    on different aggregation levels. In a first step function
    :py:func:`allocate_pv_home_batteries_to_grids` spatially distributes the
    installed battery capacities to all mv grid districts based on their
    installed pv rooftop capacity.
    Function :py:func:`allocate_home_batteries_to_buildings` further
    distributes the home battery storage systems to buildings with pv
    rooftop systems.

    *Dependencies*
      * :py:func:`download_mastr_data <egon.data.datasets.mastr.download_mastr_data>`
      * :py:func:`define_mv_grid_districts <egon.data.datasets.mv_grid_districts.define_mv_grid_districts>`
      * :py:class: `PowerPlants <egon.data.datasets.power_plants.PowerPlants>`
      * :py:class:`ScenarioCapacities <egon.data.datasets.scenario_capacities.ScenarioCapacities>`
      * :py:class:`ScenarioParameters <egon.data.datasets.scenario_parameters.ScenarioParameters>`
      * :py:class:`Vg250MvGridDistricts <egon.data.datasets.vg250_mv_grid_districts.Vg250MvGridDistricts>`

    *Resulting tables*
      * :py:class:`supply.egon_storages <egon.data.datasets.storages.EgonStorages>`

    """

    #:
    name: str = "Storages"
    #:
    version: str = "0.0.13"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                create_tables,
                allocate_pumped_hydro_scn,
                allocate_other_storage_units,
                allocate_pv_home_batteries_to_grids,
                allocate_home_batteries_to_buildings,
            ),
        )


def create_tables():
    """Create tables for power plant data
    Returns
    -------
    None.
    """
    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS supply;")
    engine = db.engine()
    db.execute_sql(f"""DROP TABLE IF EXISTS
        {Storages.targets.tables['storages']}""")

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

    nep = select_nep_pumped_hydro(scn=scn)
    mastr = select_mastr_pumped_hydro()

    # Assign voltage level to MaStR
    mastr["voltage_level"] = assign_voltage_level(
        mastr.rename({"el_capacity": "Nettonennleistung"}, axis=1),
        Storages.sources,
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
        located, unmatched = get_location(nep, scn)

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
    SELECT * FROM {Storages.sources.tables['egon_mv_grid_district']}
    """,
        epsg=4326,
    )

    ehv_grid_districts = db.select_geodataframe(
        f"""
    SELECT * FROM {Storages.sources.tables['ehv_voronoi']}
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
    db.execute_sql(f""" DELETE FROM {Storages.targets.tables['storages']}
        WHERE carrier IN ('pumped_hydro')
        AND scenario='{scn}';""")

    # If export = True export pumped_hydro plants to data base

    if export:
        # Insert into target table
        with session_scope() as session:
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


def allocate_storage_units_sq(scn_name, storage_types):
    """
    Allocate storage units by mastr data only. Capacities outside
    germany are assigned to foreign buses.

    Parameters
    ----------
    scn_name: str
        Scenario name
    storage_types: list
        contains all the required storage units carriers to be imported

    Returns
    -------

    """
    # NOTE: previously derived from get_sector_parameters(...)["weather_year"],
    # which is a fixed representative meteorological year (e.g. 2011) used
    # for feed-in time series - not the scenario's real calendar reference
    # date. That mismatch silently filtered out almost all real storage
    # units. SCENARIO_TIMESTAMP holds the actual per-scenario reference date.
    scenario_date_max = SCENARIO_TIMESTAMP[scn_name].strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    map_storage = {
        # "battery" is now dead: this function is only ever called with
        # storage_types=["pumped_hydro"] (see allocate_pumped_hydro_scn()).
        # Real battery storage is handled by allocate_battery_storage()
        # instead, reading from egon_power_plants_storage rather than
        # re-parsing this CSV. "compressed_air"/"flywheel"/"other" were
        # already no used before 
        "battery": "Batterie",
        "pumped_hydro": "Pumpspeicher",
        "compressed_air": "Druckluft",
        "flywheel": "Schwungrad",
        "other": "Sonstige",
    }

    for storage_type in storage_types:
        # Read-in data from MaStR
        mastr_ph = pd.read_csv(
            Storages.sources.files["mastr_storage"],
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
                "DatumEndgueltigeStilllegung",
                "Inbetriebnahmedatum",
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
                "DatumEndgueltigeStilllegung": "decommissioning_date",
            }
        )

        # Select only the required type of storage
        mastr_ph = mastr_ph.loc[mastr_ph.carrier == map_storage[storage_type]]

        # Select only storage units in operation
        mastr_ph.loc[
            mastr_ph["decommissioning_date"] > scenario_date_max,
            "EinheitBetriebsstatus",
        ] = "InBetrieb"
        mastr_ph = mastr_ph.loc[
            mastr_ph.EinheitBetriebsstatus.isin(
                ["InBetrieb", "VoruebergehendStillgelegt"]
            )
        ]

        # Select only storage units installed before scenario_date_max
        mastr_ph = mastr_ph[
            pd.to_datetime(mastr_ph["Inbetriebnahmedatum"]) < scenario_date_max
        ]

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

        # Remove all PP without geocord
        mastr_ph = mastr_ph.dropna(subset="Laengengrad")

        # Get geometry of villages/cities with same name of pp with missing geocord
        with session_scope() as session:
            query = session.query(Vg250GemClean.gen, Vg250GemClean.geometry)
            df_cities = gpd.read_postgis(
                query.statement,
                query.session.bind,
                geom_col="geometry",
                crs="3035",
            )

        # Keep only useful cities
        df_cities = df_cities[df_cities["gen"].isin(mastr_ph_nogeo["city"])]

        # Just take the first entry, inaccuracy is negligible as centroid is taken afterwards
        df_cities = df_cities.drop_duplicates("gen", keep="first")

        # Use the centroid instead of polygon of region
        df_cities.loc[:, "geometry"] = df_cities["geometry"].centroid

        # Change coordinate system
        df_cities.to_crs("4326", inplace=True)

        # Add centroid geometry to pp without geometry
        mastr_ph_nogeo = pd.merge(
            left=df_cities,
            right=mastr_ph_nogeo,
            right_on="city",
            left_on="gen",
            suffixes=("", "_no-geo"),
            how="inner",
        ).drop("gen", axis=1)

        mastr_ph = pd.concat([mastr_ph, mastr_ph_nogeo], axis=0)

        # aggregate capacity per location
        agg_cap = mastr_ph.groupby("geometry")["el_capacity"].sum()

        # list mastr number by location
        agg_mastr = mastr_ph.groupby("geometry")["EinheitMastrNummer"].apply(
            list
        )

        # remove duplicates by location
        mastr_ph = mastr_ph.drop_duplicates(
            subset="geometry", keep="first"
        ).drop(["el_capacity", "EinheitMastrNummer"], axis=1)

        # Adjust capacity
        mastr_ph = pd.merge(
            left=mastr_ph,
            right=agg_cap,
            left_on="geometry",
            right_on="geometry",
        )

        # Adjust capacity
        mastr_ph = pd.merge(
            left=mastr_ph,
            right=agg_mastr,
            left_on="geometry",
            right_on="geometry",
        )

        # Drop small pp <= 30 kW
        mastr_ph = mastr_ph.loc[mastr_ph["el_capacity"] > 0.03]

        # Apply voltage level by capacity
        mastr_ph = apply_voltage_level_thresholds(mastr_ph)
        mastr_ph["voltage_level"] = mastr_ph["voltage_level"].astype(int)

        # Capacity located outside germany -> will be assigned to foreign buses
        mastr_ph_foreign = mastr_ph.loc[mastr_ph["federal_state"].isna()]

        # Keep only capacities within germany
        mastr_ph = mastr_ph.dropna(subset="federal_state")

        # In test mode, keep only storage units within the active dataset
        # boundary (mirrors select_mastr_pumped_hydro() in pumped_hydro.py).
        # Re-cast to a proper GeoDataFrame first: the preceding pd.concat()/
        # pd.merge() calls silently degrade mastr_ph back to a plain
        # DataFrame, which would send filter_mastr_geometry() down the
        # wrong (Laengengrad/Breitengrad-rebuild) code path.
        if (
            config.settings()["egon-data"]["--dataset-boundary"]
            == "Schleswig-Holstein"
        ):
            mastr_ph = gpd.GeoDataFrame(
                mastr_ph, geometry="geometry", crs="EPSG:4326"
            )
            mastr_ph = filter_mastr_geometry(
                mastr_ph, federal_state="SchleswigHolstein"
            )

            # mastr_ph_foreign is split off by a missing federal_state text
            # field, not by actual geo-location - apply the same spatial
            # filter here too, otherwise plants with a missing Bundesland
            # entry (regardless of their real location) bypass the
            # test-mode boundary entirely via the foreign-bus assignment
            # below
            mastr_ph_foreign = gpd.GeoDataFrame(
                mastr_ph_foreign, geometry="geometry", crs="EPSG:4326"
            )
            mastr_ph_foreign = filter_mastr_geometry(
                mastr_ph_foreign, federal_state="SchleswigHolstein"
            )

        # Asign buses within germany
        mastr_ph = assign_bus_id(
            mastr_ph, sources=Storages.sources, drop_missing=True
        )
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
            df_foreign_buses.set_index("bus_id")
            .loc[central_bus[0], "geom"]
            .values
        )
        df_foreign_buses = df_foreign_buses[
            df_foreign_buses["geom"].isin(central_bus["geom"])
        ]

        if len(mastr_ph_foreign) > 0:
            # Assign closest bus at voltage level to foreign pp
            nearest_neighbors = []
            for vl, v_nom in {1: 380, 2: 220, 3: 110}.items():
                ph = mastr_ph_foreign.loc[
                    mastr_ph_foreign["voltage_level"] == vl
                ]
                if ph.empty:
                    continue
                bus = df_foreign_buses.loc[
                    df_foreign_buses["v_nom"] == v_nom,
                    ["v_nom", "country", "bus_id", "geom"],
                ]
                results = gpd.sjoin_nearest(
                    left_df=ph,
                    right_df=bus,
                    how="left",
                    distance_col="distance",
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
        mastr_ph["carrier"] = storage_type
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
        db.execute_sql(f""" DELETE FROM supply.egon_storages
            WHERE carrier = '{storage_type}'
            AND scenario = '{scn_name}'
            AND sources ->> 'el_capacity' = 'MaStR aggregated by location';""")

        with db.session_scope() as session:
            session.bulk_insert_mappings(
                EgonStorages,
                mastr_ph.to_dict(orient="records"),
            )



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

    dataset = config.settings()["egon-data"]["--dataset-boundary"]

    if scenario == "eGon2035":
        target_file = (
            Path(".")
            / "data_bundle_egon_data"
            / "nep2035_version2021"
            / Storages.sources.files["nep_capacities"]
        )

        capacities_nep = pd.read_excel(
            target_file,
            sheet_name="1.Entwurf_NEP2035_V2021",
            index_col="Unnamed: 0",
        )

        # Select national target value in MW
        target = capacities_nep.Summe["PV-Batteriespeicher"] * 1000

        if dataset == "Schleswig-Holstein":
            # break down national target to SH's rough share
            target = target / 16

    else:
        target_df = db.select_dataframe(f"""
            SELECT capacity
            FROM {Storages.sources.tables['capacities']}
            WHERE scenario_name = '{scenario}'
            AND carrier = 'battery';
            """)

        # Sum over all returned federal states: status quo has a single
        # national row (nuts='DE'), reGon2037/reGon2045 have one row per
        # federal state which is already scoped to the active
        # --dataset-boundary
        target = target_df.capacity.sum()

        if "status" in scenario and dataset == "Schleswig-Holstein":
            # status quo target is always national (nuts='DE'), still
            # needs to be broken down to SH's rough share in test mode
            target = target / 16

    pv_rooftop = db.select_dataframe(f"""
        SELECT bus, p_nom, generator_id
        FROM {Storages.sources.tables['generators']}
        WHERE scn_name = '{scenario}'
        AND carrier = 'solar_rooftop'
        AND bus IN
            (SELECT bus_id FROM {Storages.sources.tables['bus']}
               WHERE scn_name = '{scenario}' AND country = 'DE' );
        """)

    battery = pv_rooftop
    battery["p_nom_min"] = target * battery["p_nom"] / battery["p_nom"].sum()
    battery = battery.drop(columns=["p_nom"])

    # Subtract already-existing real battery capacity per bus from the 
    # NEP target to avoid double-counting real + modeled capacity at the 
    # same bus. SO just modeled capacities are spartially distributed 
    # according PV-capacities
    real_capacity = db.select_dataframe(f"""
        SELECT bus_id AS bus, sum(el_capacity) AS real_capacity
        FROM {Storages.targets.tables['storages']}
        WHERE carrier = 'home_battery'
        AND scenario = '{scenario}'
        AND sources ->> 'el_capacity' = 'MaStR'
        GROUP BY bus_id;
        """)

    battery = battery.merge(real_capacity, on="bus", how="left")
    battery["real_capacity"] = battery["real_capacity"].fillna(0)

    over_covered = battery["real_capacity"] > battery["p_nom_min"]
    if over_covered.any():
        logger.warning(
            f"In {over_covered.sum()} grid(s) in scenario {scenario}, real "
            f"home battery capacity already exceeds the modeled target. "
            f"No additional (modeled) capacity will be added there."
        )

    battery["p_nom_min"] = (
        battery["p_nom_min"] - battery["real_capacity"]
    ).clip(lower=0)
    battery = battery.drop(columns=["real_capacity"])

    battery["carrier"] = "home_battery"
    battery["scenario"] = scenario

    source = "NEP"

    battery["source"] = (
        f"{source} capacity allocated based in installed PV rooftop capacity"
    )

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


def allocate_battery_storage(scn_name):
    """
    Allocate real battery storage units from MaStR (supply.egon_power_plants_storage)
    for the given scenario. Split into two carriers by grid connection voltage
    level:
      * 'home_battery' (voltage_level 6, 7 - LV / building-connected) - carried
        forward into all scenarios, aged with the scenario-specific assumed
        battery storage lifetime (see determine_end_of_life_gens() below).
      * 'BESS' (voltage_level 1-5 - MV and above, grid-scale battery energy
        storage systems, not tied to individual buildings) - like
        home_battery, real capacity is now aged and carried forward into
        every scenario. A modeled/residual BESS component analogous to
        home_batteries_per_scenario() (target minus real capacity,
        distributed spatially) is deliberately not implemented (decision
        2026-08-31): eTraGo already allows extendable BESS capacity above
        this real-capacity floor at every substation bus (see
        storages_etrago.extendable_batteries_per_scenario()), so further
        capacity growth is left to eTraGo's own cost optimization rather
        than a hand-modeled spatial heuristic here. An explicit lower-bound
        constraint tied to the NEP 'Großbatteriespeicher' target in
        egon_scenario_capacities (currently unused) is a possible future
        enhancement on the eTraGo side, not planned for now.
    """

    scenario_date_max = SCENARIO_TIMESTAMP[scn_name].strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    sql = """
        SELECT gens_id AS source_id, capacity AS el_capacity, voltage_level,
               bus_id, commissioning_date, decommissioning_date, geom
        FROM supply.egon_power_plants_storage
        WHERE technology = 'Batterie'
    """
    mastr = db.select_geodataframe(sql, geom_col="geom", epsg=4326)

    mastr["commissioning_date"] = pd.to_datetime(mastr["commissioning_date"], errors="coerce")
    mastr.loc[mastr["commissioning_date"] < "1990-01-01", "commissioning_date"] = pd.NaT
    decommissioning_date = pd.to_datetime(mastr["decommissioning_date"], errors="coerce")

    # keep only units already commissioned and not (yet) decommissioned
    # as of the scenario's reference date
    mastr = mastr.loc[
        (mastr["commissioning_date"] < scenario_date_max)
        & (decommissioning_date.isna() | (decommissioning_date > scenario_date_max))
    ]

    # Age units against the scenario's own assumed battery storage lifetime
    # (applied uniformly, including status2024, to also weed out implausibly
    # old registrations there). Real, reported decommissionings are handled
    # above already; this additionally covers units MaStR still lists as
    # "in Betrieb" but that are statistically past their expected lifetime.
    lifetime = pd.Timedelta(
        get_sector_parameters("electricity", scn_name)["lifetime"][
            "BESS storage"
        ]
        * 365,
        unit="D",
    )
    # determine_end_of_life_gens() expects a "capacity" column (PV
    # convention); rename around the call, for batteries it is "el_capacity".
    mastr = determine_end_of_life_gens(
        mastr.rename(columns={"el_capacity": "capacity"}),
        SCENARIO_TIMESTAMP[scn_name].tz_localize(None),
        lifetime,
    ).rename(columns={"capacity": "el_capacity"})
    mastr = mastr.loc[~mastr.end_of_life].drop(columns=["age", "end_of_life"])

    mastr["carrier"] = "BESS"
    mastr.loc[mastr.voltage_level.isin([6, 7]), "carrier"] = "home_battery"

    mastr["scenario"] = scn_name
    mastr["source_id"] = mastr["source_id"].apply(lambda x: {"MastrNummer": x})
    mastr["sources"] = [{"el_capacity": "MaStR"}] * mastr.shape[0]

    db.execute_sql(f"""
        DELETE FROM supply.egon_storages
        WHERE carrier IN ('BESS', 'home_battery')
        AND scenario = '{scn_name}'
        AND sources ->> 'el_capacity' = 'MaStR';""")

    with db.session_scope() as session:
        session.bulk_insert_mappings(
            EgonStorages,
            mastr.assign(geom=mastr["geom"].apply(lambda x: x.wkb_hex))[
                [
                    "source_id",
                    "el_capacity",
                    "voltage_level",
                    "bus_id",
                    "carrier",
                    "scenario",
                    "commissioning_date",
                    "geom",
                    "sources",
                ]
            ].to_dict(orient="records"),
        )

    return mastr


def allocate_pv_home_batteries_to_grids():
    for scn in config.settings()["egon-data"]["--scenarios"]:
        home_batteries_per_scenario(scn)


def allocate_pumped_hydro_scn():
    for scn in config.settings()["egon-data"]["--scenarios"]:
        if "status" in scn:
            allocate_storage_units_sq(
                scn_name=scn, storage_types=["pumped_hydro"]
            )
        else:
            allocate_pumped_hydro(scn=scn, export=True)


def allocate_other_storage_units():
    # Both 'BESS' and 'home_battery' are aged and carried forward into
    # every scenario now (real MaStR capacity only for BESS - no modeled
    # residual component yet, see #1478).
    for scn in config.settings()["egon-data"]["--scenarios"]:
        allocate_battery_storage(scn_name=scn)


