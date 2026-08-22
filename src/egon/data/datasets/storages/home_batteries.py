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
from sqlalchemy import Column, Float, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets import load_sources_and_targets
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


def _load_buildings_with_geom(building_ids):
    """
    Load point geometries for a set of building IDs from both OSM buildings
    tables (osm_buildings_filtered uses bigint IDs, osm_buildings_synthetic
    uses text IDs that are numeric strings and can be cast to bigint).
    """
    ids = ",".join(str(int(b)) for b in building_ids)

    filtered = db.select_geodataframe(
        f"""
        SELECT id AS building_id, geom_point AS geom
        FROM openstreetmap.osm_buildings_filtered
        WHERE id IN ({ids});
        """,
        geom_col="geom",
        epsg=4326,
    )
    synthetic = db.select_geodataframe(
        f"""
        SELECT id::bigint AS building_id, geom_point AS geom
        FROM openstreetmap.osm_buildings_synthetic
        WHERE id::bigint IN ({ids});
        """,
        geom_col="geom",
        epsg=4326,
    )

    return gpd.GeoDataFrame(
        pd.concat([filtered, synthetic], ignore_index=True),
        geometry="geom",
        crs=4326,
    )


def _load_buildings_in_grid(bus_id):
    """
    Fallback candidate pool: all buildings (with or without pv) located
    within the mv grid district of bus_id.
    """
    return db.select_geodataframe(
        f"""
        SELECT b.id AS building_id, b.geom_point AS geom
        FROM openstreetmap.osm_buildings_filtered b, grid.egon_mv_grid_district g
        WHERE g.bus_id = {bus_id} AND ST_Within(b.geom_point, g.geom)
        UNION ALL
        SELECT s.id::bigint AS building_id, s.geom_point AS geom
        FROM openstreetmap.osm_buildings_synthetic s, grid.egon_mv_grid_district g
        WHERE g.bus_id = {bus_id} AND ST_Within(s.geom_point, g.geom);
        """,
        geom_col="geom",
        epsg=4326,
    )


def match_real_batteries_to_buildings(scenario):
    """
    Match real (MaStR) home batteries to buildings via nearest-neighbor
    spatial matching within their grid. Prefers pv-equipped buildings;
    falls back to the nearest building overall in that grid once pv
    candidates run out, so every real battery still gets a building.
    """
    columns = [
        "scenario",
        "bus_id",
        "building_id",
        "capacity",
        "p_nom",
        "sources",
    ]

    real_batteries = db.select_geodataframe(
        f"""
        SELECT bus_id, el_capacity, geom
        FROM supply.egon_storages
        WHERE carrier = 'home_battery'
        AND scenario = '{scenario}'
        AND sources ->> 'el_capacity' = 'MaStR';
        """,
        geom_col="geom",
        epsg=4326,
    )

    if real_batteries.empty:
        return pd.DataFrame(columns=columns)

    pv_buildings_df = db.select_dataframe(f"""
        SELECT DISTINCT building_id, bus_id
        FROM supply.egon_power_plants_pv_roof_building
        WHERE scenario = '{scenario}';
        """)
    pv_geoms = _load_buildings_with_geom(pv_buildings_df.building_id.unique())
    pv_buildings = gpd.GeoDataFrame(
        pv_buildings_df.merge(pv_geoms, on="building_id"),
        geometry="geom",
        crs=4326,
    )

    cbat_pbat_ratio = get_sector_parameters("electricity", scenario)[
        "efficiency"
    ]["battery"]["max_hours"]

    matched = []

    for bus_id, remaining in real_batteries.groupby("bus_id"):
        candidates = pv_buildings.loc[pv_buildings.bus_id == bus_id].copy()

        while not remaining.empty:
            if candidates.empty:
                candidates = _load_buildings_in_grid(bus_id)

                if candidates.empty:
                    logger.warning(
                        f"No building found in grid {bus_id} to match "
                        f"{len(remaining)} real home batteries to; skipped."
                    )
                    break

            nn = (
                gpd.sjoin_nearest(
                    remaining,
                    candidates[["building_id", "geom"]],
                    distance_col="dist",
                )
                .sort_values("dist")
                .drop_duplicates(subset="building_id", keep="first")
            )

            matched.append(nn)

            remaining = remaining.drop(index=nn.index)
            candidates = candidates.loc[
                ~candidates.building_id.isin(nn.building_id)
            ]

    if not matched:
        return pd.DataFrame(columns=columns)

    result = pd.concat(matched, ignore_index=True)

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
        # Match real batteries to their nearest building first, so the
        # modeled (residual) sampling below can exclude buildings already
        # taken and never assigns both a real and a modeled battery to the
        # same building.
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
            pv_df = db.select_dataframe(sql.format(scenario, bus_id))
            pv_df = pv_df.loc[~pv_df.building_id.isin(used_building_ids)]

            pv_sum = pv_df.capacity.sum()

            if pv_sum > 0:
                grid_ratio = bat_cap / pv_sum
            else:

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
    )
