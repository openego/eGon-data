"""
Home Battery allocation to buildings

Main module for allocation of home batteries onto buildings and sizing them
depending on pv rooftop system size.

**Contents of this module**

* Creation of DB tables
* Allocate given home battery capacity per mv grid to buildings with pv rooftop
  systems. The sizing of the home battery system depends on the size of the
  pv rooftop system and can be set within the *datasets.yml*. Default sizing is
  1:1 between the pv rooftop capacity (kWp) and the battery capacity (kWh).
* Write results to DB

**Configuration**

The config of this dataset can be found in *datasets.yml* in section
*home_batteries*.

**Scenarios and variations**

Assumptions can be changed within the *datasets.yml*.

Only buildings with a pv rooftop systems are considered within the allocation
process. The default sizing of home batteries is 1:1 between the pv rooftop
capacity (kWp) and the battery capacity (kWh). Reaching the exact value of the
allocation of battery capacities per grid area leads to slight deviations from
this specification.

## Methodology

The selection of buildings is done randomly until a result is reached which is
close to achieving the sizing specification.
"""

import datetime
import json

from loguru import logger
from numpy.random import RandomState
from omi.dialects import get_dialect
from scipy.spatial import cKDTree
from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets import load_sources_and_targets
from egon.data.datasets.power_plants.pv_rooftop_buildings import (
    MAX_EXACT_PV_DISTANCE,
    N_EXACT_PV_CANDIDATES,
)
from egon.data.datasets.scenario_parameters import get_sector_parameters
from egon.data.metadata import (
    context,
    contributors,
    generate_resource_fields_from_db_table,
    license_dedl,
    license_odbl,
    meta_metadata,
    sources,
)

Base = declarative_base()

# This block is added because they are constant and needs to be independant from config.dataset
CONSTANTS = {
    "cbat_ppv_ratio": 1,
    "rtol": 0.05,
    "max_it": 100,
    "deposit_id_mastr": 10491882,
    "deposit_id_data_bundle": 16576506,
}


def get_cbat_pbat_ratio():
    """
    Mean ratio between the storage capacity and the power of the pv rooftop
    system

    Returns
    -------
    int
        Mean ratio between the storage capacity and the power of the pv
        rooftop system
    """
    sources, targets = load_sources_and_targets("Storages")

    sql = f"""
    SELECT max_hours
    FROM {sources.tables["etrago_storage"]}
    WHERE carrier = 'home_battery'
    """

    return int(db.select_dataframe(sql).iat[0, 0])


def _load_pv_buildings_with_geom(scenario):
    """
    Load all buildings with pv rooftop plants of a scenario together with
    their point geometry from both OSM buildings tables
    (osm_buildings_filtered uses bigint IDs, osm_buildings_synthetic uses
    text IDs that are numeric strings and can be cast to bigint).

    The join is done in the database, because passing millions of building
    IDs as an IN-list exceeds the postgres memory limit of 1 GB (#1571).
    """
    return db.select_geodataframe(
        f"""
        WITH pv AS (
            SELECT DISTINCT building_id, bus_id
            FROM supply.egon_power_plants_pv_roof_building
            WHERE scenario = '{scenario}'
        )
        SELECT pv.building_id, pv.bus_id, b.geom_point AS geom
        FROM pv
        JOIN openstreetmap.osm_buildings_filtered b
            ON b.id = pv.building_id
        UNION ALL
        SELECT pv.building_id, pv.bus_id, s.geom_point AS geom
        FROM pv
        JOIN openstreetmap.osm_buildings_synthetic s
            ON s.id::bigint = pv.building_id;
        """,
        geom_col="geom",
        epsg=3035,
    )


def _nearest_free_buildings(points, tree, free, max_distance, n_candidates):
    """
    Assign points in the given order to the nearest free building.

    Like allocate_exact_pv() for pv plants: the n_candidates nearest
    buildings within max_distance are considered. If all of them are taken
    already, the number of candidates is increased.

    Parameters
    ----------
    points : numpy.ndarray
        Coordinates (EPSG:3035) of the points, shape (n, 2).
    tree : scipy.spatial.cKDTree
        Tree of the building coordinates (EPSG:3035).
    free : numpy.ndarray
        Boolean mask of free buildings (positions in tree), updated in place.
    max_distance : float
        Maximum distance in m between point and building.
    n_candidates : int
        Number of nearest buildings considered in the first search.

    Returns
    -------
    numpy.ndarray
        Position of the building in tree per point, -1 if no free building
        is found within max_distance.
    """
    result = np.full(len(points), -1, dtype=int)
    todo = np.arange(len(points))
    k = n_candidates

    while len(todo) > 0 and free.any():
        k = min(k, tree.n)
        distances, positions = tree.query(
            points[todo], k=k, distance_upper_bound=max_distance
        )
        distances = distances.reshape(len(todo), k)
        positions = positions.reshape(len(todo), k)

        retry = []
        for i, candidates, candidate_distances in zip(
            todo, positions, distances
        ):
            for pos, distance in zip(candidates, candidate_distances):
                if np.isinf(distance):
                    # no further building within max_distance
                    break
                if free[pos]:
                    free[pos] = False
                    result[i] = pos
                    break
            else:
                # all candidates are taken already
                retry.append(i)

        if k == tree.n:
            break
        todo = np.array(retry, dtype=int)
        k *= 10

    return result


def _log_assignment(scenario, step, batteries, assigned, old_bus):
    """Log number, capacity and bus changes of one assignment step."""
    moved = (assigned["bus_id"] != old_bus.loc[assigned.index]).sum()
    logger.info(
        f"Scenario {scenario}: {len(assigned)} of {len(batteries)} real home "
        f"batteries ({batteries.loc[assigned.index, 'el_capacity'].sum():.1f} "
        f"of {batteries['el_capacity'].sum():.1f} MW) assigned to buildings "
        f"by {step}, {moved} of them changed their bus."
    )


def assign_real_home_batteries_to_buildings(batteries, scenario):
    """
    Assign real MaStR home batteries to buildings with pv rooftop plants
    of the scenario.

    MaStR publishes no exact coordinates for units up to 30 kW, their
    location is a substitute point per postcode and municipality. Instead
    of the nearest building to that point, a battery is assigned to the
    building of the pv plant that belongs to it according to MaStR:

    1. Same location (LokationMastrNummer): the battery and the pv plant
       are connected at the same grid connection point. If the location
       has more than one pv plant, the building of the largest one is used.
    2. Same operator (AnlagenbetreiberMastrNummer), if the operator has
       pv plants on exactly one building in the scenario.

    Several batteries can be assigned to the same building in these steps,
    if MaStR lists them at the same location or with the same operator.

    The remaining batteries are assigned to free pv buildings, i.e.
    buildings without a battery yet, one battery per building:

    3. Exact coordinates (not geocoded): nearest free pv building within
       MAX_EXACT_PV_DISTANCE, in descending order of capacity (like
       allocate_exact_pv() for pv plants).
    4. Municipality of the (substitute) point: random free pv buildings of
       the municipality, weighted by their pv capacity. Larger batteries
       get the buildings with larger pv plants.
    5. Nearest free pv building, also in a neighbouring municipality or
       grid district, if the municipality has no free pv building left.

    Parameters
    ----------
    batteries : geopandas.GeoDataFrame
        Real home batteries with columns source_id, el_capacity, bus_id,
        location_mastr_id, operator_mastr_id, geometry_geocoded and geom
        (EPSG:4326).
    scenario : str
        Scenario name.

    Returns
    -------
    geopandas.GeoDataFrame
        Assigned batteries only (index of `batteries`) with columns
        building_id, bus_id and geom (EPSG:4326) of the building.
    """
    sources, _ = load_sources_and_targets("Storages")

    # Buildings with real pv plants of the scenario and the MaStR ids of
    # these plants
    pv = db.select_dataframe(
        f"""
        SELECT rb.building_id, rb.capacity,
               pv.location_mastr_id, pv.operator_mastr_id
        FROM {sources.tables['pv_roof_building']} rb
        JOIN {sources.tables['mastr_pv']} pv
            ON pv.gens_id = rb.gens_id
        WHERE rb.scenario = '{scenario}'
        AND rb.bus_id IS NOT NULL;
        """
    )

    buildings = (
        _load_pv_buildings_with_geom(scenario)
        .dropna(subset=["bus_id"])
        .drop_duplicates(subset="building_id")
        .set_index("building_id")
    )
    pv = pv.loc[pv.building_id.isin(buildings.index)]

    building_id = pd.Series(np.nan, index=batteries.index)

    # 1. Same location: building of the largest pv plant at the location
    by_location = (
        pv.dropna(subset=["location_mastr_id"])
        .sort_values(["capacity", "building_id"], ascending=[False, True])
        .drop_duplicates(subset="location_mastr_id")
        .set_index("location_mastr_id")["building_id"]
    )
    building_id = building_id.fillna(
        batteries["location_mastr_id"].map(by_location)
    )
    by_location_idx = building_id.dropna().index

    # 2. Same operator with pv plants on exactly one building
    pv_operator = pv.dropna(subset=["operator_mastr_id"])
    n_buildings = pv_operator.groupby("operator_mastr_id")[
        "building_id"
    ].nunique()
    by_operator = (
        pv_operator.loc[
            pv_operator.operator_mastr_id.isin(
                n_buildings.index[n_buildings == 1]
            )
        ]
        .drop_duplicates(subset="operator_mastr_id")
        .set_index("operator_mastr_id")["building_id"]
    )
    building_id = building_id.fillna(
        batteries["operator_mastr_id"].map(by_operator)
    )
    by_operator_idx = building_id.dropna().index.difference(by_location_idx)

    # Fallback for the remaining batteries: one battery per free pv building
    building_ids = buildings.index.to_numpy()
    free = ~buildings.index.isin(building_id.dropna())
    tree = cKDTree(
        np.column_stack([buildings.geometry.x, buildings.geometry.y])
    )
    points = batteries.geometry.to_crs(3035)
    points = pd.DataFrame(
        {"x": points.x, "y": points.y}, index=batteries.index
    )

    # 3. Exact coordinates: nearest free pv building
    by_exact_idx = (
        batteries.loc[
            building_id.isna() & batteries["geometry_geocoded"].eq(False)
        ]
        .sort_values("el_capacity", ascending=False)
        .index
    )
    pos = _nearest_free_buildings(
        points.loc[by_exact_idx].to_numpy(),
        tree,
        free,
        MAX_EXACT_PV_DISTANCE,
        N_EXACT_PV_CANDIDATES,
    )
    by_exact_idx = by_exact_idx[pos >= 0]
    building_id.loc[by_exact_idx] = building_ids[pos[pos >= 0]]

    # 4. Random free pv buildings in the municipality of the point
    by_municipality_idx = []
    open_idx = building_id.index[building_id.isna()]
    if len(open_idx) > 0:
        municipalities = db.select_geodataframe(
            f"""
            SELECT ags, geometry AS geom
            FROM {sources.tables['vg250_municipalities']};
            """,
            epsg=4326,
        )
        battery_ags = gpd.sjoin(
            batteries.loc[open_idx, ["geom"]],
            municipalities,
            how="inner",
            predicate="within",
        )
        battery_ags = battery_ags.loc[
            ~battery_ags.index.duplicated(), "ags"
        ]

        pv_ags = db.select_dataframe(
            f"""
            WITH pv AS (
                SELECT building_id, SUM(capacity) AS capacity
                FROM {sources.tables['pv_roof_building']}
                WHERE scenario = '{scenario}'
                AND bus_id IS NOT NULL
                GROUP BY building_id
            )
            SELECT DISTINCT ON (pv.building_id)
                pv.building_id, pv.capacity, g.ags
            FROM pv
            JOIN {sources.tables['map_zensus_buildings']} m
                ON m.building_id = pv.building_id
            JOIN {sources.tables['map_zensus_vg250']} z
                ON z.zensus_population_id = m.zensus_population_id
            JOIN {sources.tables['vg250_municipalities']} g
                ON g.id = z.vg250_municipality_id
            ORDER BY pv.building_id, g.ags;
            """
        )
        pv_ags["pos"] = buildings.index.get_indexer(pv_ags["building_id"])
        pv_ags = pv_ags.loc[
            (pv_ags["pos"] >= 0) & pv_ags["ags"].isin(battery_ags.unique())
        ]
        candidates = {
            ags: (df["pos"].to_numpy(), df["capacity"].to_numpy())
            for ags, df in pv_ags.groupby("ags")
        }

        rng = RandomState(int(config.settings()["egon-data"]["--random-seed"]))
        for ags, group in battery_ags.groupby(battery_ags, sort=True):
            if ags not in candidates:
                continue
            pos, capacity = candidates[ags]
            pos, capacity = pos[free[pos]], capacity[free[pos]]
            n = min(len(group), len(pos))
            if n == 0:
                continue

            weights = np.maximum(capacity, 1e-6)
            chosen = rng.choice(
                len(pos), size=n, replace=False, p=weights / weights.sum()
            )
            # larger batteries get the buildings with larger pv plants
            chosen = chosen[np.argsort(-capacity[chosen], kind="stable")]
            idx = (
                batteries.loc[group.index, "el_capacity"]
                .sort_values(ascending=False, kind="stable")
                .index[:n]
            )
            building_id.loc[idx] = building_ids[pos[chosen]]
            free[pos[chosen]] = False
            by_municipality_idx.extend(idx)
    by_municipality_idx = pd.Index(by_municipality_idx)

    # 5. Nearest free pv building anywhere, e.g. in a neighbouring grid
    by_nearest_idx = (
        batteries.loc[building_id.isna()]
        .sort_values("el_capacity", ascending=False)
        .index
    )
    pos = _nearest_free_buildings(
        points.loc[by_nearest_idx].to_numpy(),
        tree,
        free,
        np.inf,
        N_EXACT_PV_CANDIDATES,
    )
    by_nearest_idx = by_nearest_idx[pos >= 0]
    building_id.loc[by_nearest_idx] = building_ids[pos[pos >= 0]]

    building_id = building_id.dropna().astype(int)
    assigned = gpd.GeoDataFrame(
        {
            "building_id": [int(b) for b in building_id],
            "bus_id": buildings.loc[building_id, "bus_id"]
            .astype(int)
            .to_numpy(),
            "geom": gpd.GeoSeries(
                buildings.loc[building_id].geometry.to_numpy(), crs=3035
            )
            .to_crs(4326)
            .to_numpy(),
        },
        index=building_id.index,
        geometry="geom",
        crs=4326,
    )

    for step, idx in [
        ("location (LokationMastrNummer)", by_location_idx),
        ("operator (AnlagenbetreiberMastrNummer)", by_operator_idx),
        ("exact coordinates (nearest free pv building)", by_exact_idx),
        ("municipality (random free pv building)", by_municipality_idx),
        ("nearest free pv building", by_nearest_idx),
    ]:
        _log_assignment(
            scenario, step, batteries, assigned.loc[idx], batteries["bus_id"]
        )

    n_open = len(batteries) - len(assigned)
    if n_open > 0:
        logger.warning(
            f"Scenario {scenario}: {n_open} real home batteries "
            f"({batteries['el_capacity'].sum() - batteries.loc[assigned.index, 'el_capacity'].sum():.1f} MW) "
            f"not assigned to a building, no free pv building left. They "
            f"keep the bus of their MaStR location."
        )

    return assigned


def match_real_batteries_to_buildings(scenario):
    """
    Real (MaStR) home batteries with their building.

    The building is assigned in allocate_battery_storage() (see
    assign_real_home_batteries_to_buildings()) and read from
    supply.egon_storages here, so that bus and building of a real battery
    are the same in both tables. Batteries without a building (no free pv
    building left) are not written to supply.egon_home_batteries.
    """
    _, targets = load_sources_and_targets("Storages")

    columns = [
        "scenario",
        "bus_id",
        "building_id",
        "capacity",
        "p_nom",
        "sources",
    ]

    real_batteries = db.select_dataframe(
        f"""
        SELECT bus_id, building_id, el_capacity
        FROM {targets.tables['storages']}
        WHERE carrier = 'home_battery'
        AND scenario = '{scenario}'
        AND sources ->> 'el_capacity' = 'MaStR';
        """
    )

    without_building = real_batteries.building_id.isna()
    if without_building.any():
        logger.warning(
            f"Scenario {scenario}: {without_building.sum()} real home "
            f"batteries "
            f"({real_batteries.loc[without_building, 'el_capacity'].sum():.3f}"
            f" MW) without building are missing in egon_home_batteries."
        )
    # building_id is read as float because of missing values
    result = real_batteries.loc[~without_building].astype(
        {"building_id": int}
    )

    if result.empty:
        return pd.DataFrame(columns=columns)

    cbat_pbat_ratio = get_sector_parameters("electricity", scenario)[
        "efficiency"
    ]["battery"]["max_hours"]

    return pd.DataFrame(
        {
            "scenario": scenario,
            "bus_id": result.bus_id,
            "building_id": result.building_id,
            "p_nom": result.el_capacity,
            "capacity": result.el_capacity * cbat_pbat_ratio,
            "sources": [{"el_capacity": "MaStR"}] * len(result),
        }
    )


def allocate_home_batteries_to_buildings():
    """
    Allocate home battery storage systems to buildings with pv rooftop systems
    """
    sources, targets = load_sources_and_targets("Storages")

    scenarios = config.settings()["egon-data"]["--scenarios"]

    cbat_ppv_ratio = CONSTANTS["cbat_ppv_ratio"]
    rtol = CONSTANTS["rtol"]
    max_it = CONSTANTS["max_it"]

    df_list = []

    for scenario in scenarios:
        # Real batteries with their building first, so the modeled
        # (residual) sampling below can exclude buildings already taken and
        # never assigns both a real and a modeled battery to the same
        # building.
        real_matches = match_real_batteries_to_buildings(scenario)
        df_list.append(real_matches)
        used_building_ids = set(real_matches.building_id)

        # get home battery capacity per mv grid id
        # Only the modeled (residual) rows - real batteries (tagged
        # sources->>'el_capacity'='MaStR') are handled separately above and
        # matched directly to their own building rather than sampled here.
        sql = f"""
        SELECT el_capacity as p_nom_min, bus_id as bus FROM
        {targets.tables["storages"]}
        WHERE carrier = 'home_battery'
        AND scenario = '{scenario}'
        AND sources ->> 'el_capacity' != 'MaStR';
        """
        cbat_pbat_ratio = get_sector_parameters("electricity", scenario)[
            "efficiency"
        ]["battery"]["max_hours"]

        home_batteries_df = db.select_dataframe(sql)

        home_batteries_df = home_batteries_df.assign(
            bat_cap=home_batteries_df.p_nom_min * cbat_pbat_ratio
        )

        sql = """
        SELECT building_id, capacity
        FROM supply.egon_power_plants_pv_roof_building
        WHERE scenario = '{}'
        AND bus_id = {}
        """

        for bus_id, bat_cap in home_batteries_df[
            ["bus", "bat_cap"]
        ].itertuples(index=False):
            # no building rows with 0 MW
            if not bat_cap > 0:
                continue

            pv_df = db.select_dataframe(sql.format(scenario, bus_id))
            pv_df = pv_df.loc[~pv_df.building_id.isin(used_building_ids)]

            pv_sum = pv_df.capacity.sum()

            if pv_sum > 0:
                grid_ratio = bat_cap / pv_sum
            else:
                if bat_cap > 0:
                    logger.error(
                        f"No free pv building in grid {bus_id} and scenario "
                        f"{scenario}; modeled home battery capacity of "
                        f"{bat_cap / cbat_pbat_ratio:.3f} MW is missing in "
                        f"egon_home_batteries."
                    )
                continue

            if grid_ratio > cbat_ppv_ratio:
                logger.warning(
                    f"In Grid {bus_id} and scenario {scenario}, the ratio of "
                    f"home storage capacity to pv rooftop capacity is above 1"
                    f" ({grid_ratio: g}). The storage capacity of pv rooftop "
                    f"systems will be high."
                )

            if grid_ratio < cbat_ppv_ratio:
                random_state = RandomState(seed=bus_id)

                n = max(int(len(pv_df) * grid_ratio), 1)

                best_df = pv_df.sample(n=n, random_state=random_state)

                i = 0

                while (
                    not np.isclose(best_df.capacity.sum(), bat_cap, rtol=rtol)
                    and i < max_it
                ):
                    sample_df = pv_df.sample(n=n, random_state=random_state)

                    if abs(best_df.capacity.sum() - bat_cap) > abs(
                        sample_df.capacity.sum() - bat_cap
                    ):
                        best_df = sample_df.copy()

                    i += 1

                    if sample_df.capacity.sum() < bat_cap:
                        n = min(n + 1, len(pv_df))
                    else:
                        n = max(n - 1, 1)

                if not np.isclose(best_df.capacity.sum(), bat_cap, rtol=rtol):
                    logger.warning(
                        f"No suitable generators could be found in Grid "
                        f"{bus_id} and scenario {scenario} to achieve the "
                        f"desired ratio between battery capacity and pv "
                        f"rooftop capacity. The ratio will be "
                        f"{bat_cap / best_df.capacity.sum()}."
                    )

                pv_df = best_df.copy()

            bat_df = pv_df.drop(columns=["capacity"]).assign(
                capacity=pv_df.capacity / pv_df.capacity.sum() * bat_cap,
                p_nom=pv_df.capacity
                / pv_df.capacity.sum()
                * bat_cap
                / cbat_pbat_ratio,
                scenario=scenario,
                bus_id=bus_id,
                sources=[
                    {
                        "el_capacity": "NEP capacity allocated based in "
                        "installed PV rooftop capacity"
                    }
                ]
                * len(pv_df),
            )

            df_list.append(bat_df)

    create_table(pd.concat(df_list, ignore_index=True))

    add_metadata()


class EgonHomeBatteries(Base):
    __tablename__ = "egon_home_batteries"
    __table_args__ = {"schema": "supply"}

    index = Column(Integer, primary_key=True, index=True)
    scenario = Column(String)
    bus_id = Column(Integer)
    building_id = Column(Integer)
    p_nom = Column(Float)
    capacity = Column(Float)
    sources = Column(JSONB)


def add_metadata():
    """
    Add metadata to table supply.egon_home_batteries
    """
    _, targets = load_sources_and_targets("Storages")

    deposit_id_mastr = CONSTANTS["deposit_id_mastr"]
    deposit_id_data_bundle = CONSTANTS["deposit_id_data_bundle"]

    contris = contributors(["kh", "kh"])

    contris[0]["date"] = "2023-03-15"

    contris[0]["object"] = "metadata"
    contris[1]["object"] = "dataset"

    contris[0]["comment"] = "Add metadata to dataset."
    contris[1]["comment"] = "Add workflow to generate dataset."

    meta = {
        "name": targets.get_table_name("home_batteries"),
        "title": "eGon Home Batteries",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": "Home storage systems allocated to buildings",
        "language": "en-US",
        "keywords": ["battery", "batteries", "home", "storage", "building"],
        "publicationDate": datetime.date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": "none",
            "extent": "Germany",
            "resolution": "building",
        },
        "temporal": {
            "referenceDate": "2021-12-31",
            "timeseries": {},
        },
        "sources": [
            {
                "title": "Data bundle for egon-data",
                "description": (
                    "Data bundle for egon-data: A transparent and "
                    "reproducible data processing pipeline for energy "
                    "system modeling"
                ),
                "path": (
                    "https://zenodo.org/record/"
                    f"{deposit_id_data_bundle}#.Y_dWM4CZMVM"
                ),
                "licenses": [license_dedl(attribution="© Cußmann, Ilka")],
            },
            {
                "title": ("open-MaStR power unit registry for eGo^n project"),
                "description": (
                    "Data from Marktstammdatenregister (MaStR) data using "
                    "the data dump from 2022-11-17 for eGon-data."
                ),
                "path": (f"https://zenodo.org/record/{deposit_id_mastr}"),
                "licenses": [license_dedl(attribution="© Amme, Jonathan")],
            },
            # 'sources()' correctly refers to the function from metadata
            sources()["openstreetmap"],
            sources()["era5"],
            sources()["vg250"],
            sources()["egon-data"],
            sources()["nep2021"],
            sources()["mastr"],
            sources()["technology-data"],
        ],
        "licenses": [license_odbl("© eGon development team")],
        "contributors": contris,
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": targets.get_table_name("home_batteries"),
                "path": "None",
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": generate_resource_fields_from_db_table(
                        targets.get_table_schema("home_batteries"),
                        # FIX: Use [-1] to get the table name safely (works with or without 'schema.' prefix)
                        targets.get_table_name("home_batteries").split(".")[
                            -1
                        ],
                    ),
                    "primaryKey": "index",
                },
                "dialect": {"delimiter": "", "decimalSeparator": ""},
            }
        ],
        "review": {"path": "", "badge": ""},
        "metaMetadata": meta_metadata(),
        "_comment": {
            "metadata": (
                "Metadata documentation and explanation (https://github.com/"
                "OpenEnergyPlatform/oemetadata/blob/master/metadata/v141/"
                "metadata_key_description.md)"
            ),
            "dates": (
                "Dates and time must follow the ISO8601 including time zone "
                "(YYYY-MM-DD or YYYY-MM-DDThh:mm:ss±hh)"
            ),
            "units": "Use a space between numbers and units (100 m)",
            "languages": (
                "Languages must follow the IETF (BCP47) format (en-GB, en-US, "
                "de-DE)"
            ),
            "licenses": (
                "License name must follow the SPDX License List "
                "(https://spdx.org/licenses/)"
            ),
            "review": (
                "Following the OEP Data Review (https://github.com/"
                "OpenEnergyPlatform/data-preprocessing/wiki)"
            ),
            "none": "If not applicable use (none)",
        },
    }

    dialect = get_dialect(f"oep-v{meta_metadata()['metadataVersion'][4:7]}")()

    meta = dialect.compile_and_render(dialect.parse(json.dumps(meta)))

    db.submit_comment(
        f"'{json.dumps(meta)}'",
        targets.get_table_schema("home_batteries"),
        targets.get_table_name("home_batteries").split(".")[-1],
    )


def create_table(df):
    """Create mapping table home battery <-> building id"""
    engine = db.engine()

    EgonHomeBatteries.__table__.drop(bind=engine, checkfirst=True)
    EgonHomeBatteries.__table__.create(bind=engine, checkfirst=True)

    df.reset_index().to_sql(
        name=EgonHomeBatteries.__table__.name,
        schema=EgonHomeBatteries.__table__.schema,
        con=engine,
        if_exists="append",
        index=False,
        dtype={"sources": JSONB},
    )
