"""
The charging infrastructure allocation is based on [TracBEV[(
https://github.com/rl-institut/tracbev). TracBEV is a tool for the regional allocation
of charging infrastructure. In practice this allows users to use results generated via
[SimBEV](https://github.com/rl-institut/simbev) and place the corresponding charging
points on a map. These are split into the four use cases hpc, public, home and work.
"""
from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.use_cases import (  # noqa: E501
    home,
    hpc,
    public,
    work,
)

WORKING_DIR = Path(".", "charging_infrastructure").resolve()
DATASET_CFG = config.datasets()["charging_infrastructure"]


def write_to_db(
    gdf: gpd.GeoDataFrame, mv_grid_id: int | float, use_case: str
) -> None:
    """
    Write results to charging infrastructure DB table

    Parameters
    ----------
    gdf: geopandas.GeoDataFrame
        GeoDataFrame to save
    mv_grid_id: int or float
        MV grid ID corresponding to the data
    use_case: str
        Calculated use case

    """
    if gdf.empty:
        return

    if "energy" in gdf.columns:
        gdf = gdf.assign(weight=gdf.energy.div(gdf.energy.sum()))
    else:
        rng = np.random.default_rng(DATASET_CFG["constants"]["random_seed"])

        gdf = gdf.assign(weight=rng.integers(low=0, high=100, size=len(gdf)))

        gdf = gdf.assign(weight=gdf.weight.div(gdf.weight.sum()))

    max_id = db.select_dataframe(
        """
        SELECT MAX(cp_id) FROM grid.egon_emob_charging_infrastructure
        """
    )["max"][0]

    if max_id is None:
        max_id = 0

    gdf = gdf.assign(
        cp_id=range(max_id, max_id + len(gdf)),
        mv_grid_id=mv_grid_id,
        use_case=use_case,
    )

    targets = DATASET_CFG["targets"]
    cols_to_export = targets["charging_infrastructure"]["cols_to_export"]

    gpd.GeoDataFrame(gdf[cols_to_export], crs=gdf.crs).to_postgis(
        targets["charging_infrastructure"]["table"],
        schema=targets["charging_infrastructure"]["schema"],
        con=db.engine(),
        if_exists="append",
    )


def run_tracbev():
    """
    Wrapper function to run charging infrastructure allocation
    """
    data_dict = get_data()

    run_tracbev_potential(data_dict)


def run_tracbev_potential(data_dict: dict) -> None:
    """
    Main function to run TracBEV in potential (determination of all potential
    charging points).

    Parameters
    ----------
    data_dict: dict
        Data dict containing all TracBEV run information
    """
    bounds = data_dict["boundaries"]

    for mv_grid_id in data_dict["regions"].mv_grid_id:
        region = bounds.loc[bounds.bus_id == mv_grid_id].geom

        data_dict.update({"region": region, "key": mv_grid_id})
        # Start Use Cases
        run_use_cases(data_dict)


def run_use_cases(data_dict: dict) -> None:
    """
    Run all use cases

    Parameters
    ----------
    data_dict: dict
        Data dict containing all TracBEV run information
    """
    write_to_db(
        hpc(data_dict["hpc_positions"], data_dict),
        data_dict["key"],
        use_case="hpc",
    )
    write_to_db(
        public(
            data_dict["public_positions"], data_dict["poi_cluster"], data_dict
        ),
        data_dict["key"],
        use_case="public",
    )
    write_to_db(
        work(data_dict["landuse"], data_dict["work_dict"], data_dict),
        data_dict["key"],
        use_case="work",
    )
    write_to_db(
        home(data_dict["housing_data"], data_dict),
        data_dict["key"],
        use_case="home",
    )


def get_data() -> dict[gpd.GeoDataFrame]:
    """
    Load all data necessary for TracBEV. Data loaded:

    * 'hpc_positions' - Potential hpc positions
    * 'landuse' - Potential work related positions
    * 'poi_cluster' - Potential public related positions
    * 'public_positions' - Potential public related positions
    * 'housing_data' - Potential home related positions loaded from DB
    * 'boundaries' - MV grid boundaries
    * miscellaneous found in *datasets.yml* in section *charging_infrastructure*

    Returns
    -------

    """
    tracbev_cfg = DATASET_CFG["original_data"]["sources"]["tracbev"]
    srid = tracbev_cfg["srid"]

    # TODO: get zensus housing data from DB instead of gpkg?
    files = tracbev_cfg["files_to_use"]

    data_dict = {}

    # get TracBEV files
    for f in files:
        file = WORKING_DIR / "data" / f
        name = f.split(".")[0]

        data_dict[name] = gpd.read_file(file)

        if "undefined" in data_dict[name].crs.name.lower():
            data_dict[name] = data_dict[name].set_crs(
                epsg=srid, allow_override=True
            )
        else:
            data_dict[name] = data_dict[name].to_crs(epsg=srid)

    # get housing data from DB
    sql = """
    SELECT building_id, cell_id
    FROM demand.egon_household_electricity_profile_of_buildings
    """

    df = db.select_dataframe(sql)

    count_df = (
        df.groupby(["building_id", "cell_id"])
        .size()
        .reset_index()
        .rename(columns={0: "count"})
    )

    mfh_df = (
        count_df.loc[count_df["count"] > 1]
        .groupby(["cell_id"])
        .size()
        .reset_index()
        .rename(columns={0: "num_mfh"})
    )
    efh_df = (
        count_df.loc[count_df["count"] <= 1]
        .groupby(["cell_id"])
        .size()
        .reset_index()
        .rename(columns={0: "num"})
    )

    comb_df = (
        mfh_df.merge(
            right=efh_df, how="outer", left_on="cell_id", right_on="cell_id"
        )
        .fillna(0)
        .astype(int)
    )

    sql = """
    SELECT zensus_population_id, geom as geometry
    FROM society.egon_destatis_zensus_apartment_building_population_per_ha
    """

    gdf = db.select_geodataframe(sql, geom_col="geometry", epsg=srid)

    data_dict["housing_data"] = gpd.GeoDataFrame(
        gdf.merge(
            right=comb_df, left_on="zensus_population_id", right_on="cell_id"
        ),
        crs=gdf.crs,
    ).drop(columns=["cell_id"])

    # get boundaries aka grid districts
    sql = """
    SELECT bus_id, geom FROM grid.egon_mv_grid_district
    """

    data_dict["boundaries"] = db.select_geodataframe(
        sql, geom_col="geom", epsg=srid
    )

    data_dict["regions"] = pd.DataFrame(
        columns=["mv_grid_id"],
        data=data_dict["boundaries"].bus_id.unique(),
    )

    data_dict["work_dict"] = {
        "retail": DATASET_CFG["constants"]["work_weight_retail"],
        "commercial": DATASET_CFG["constants"]["work_weight_commercial"],
        "industrial": DATASET_CFG["constants"]["work_weight_industrial"],
    }

    data_dict["sfh_available"] = DATASET_CFG["constants"][
        "single_family_home_share"
    ]
    data_dict["sfh_avg_spots"] = DATASET_CFG["constants"][
        "single_family_home_spots"
    ]
    data_dict["mfh_available"] = DATASET_CFG["constants"][
        "multi_family_home_share"
    ]
    data_dict["mfh_avg_spots"] = DATASET_CFG["constants"][
        "multi_family_home_spots"
    ]

    data_dict["random_seed"] = np.random.default_rng(
        DATASET_CFG["constants"]["random_seed"]
    )

    return data_dict
