from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.use_cases import (  # noqa: E501
    hpc,
    public,
)

WORKING_DIR = Path(".", "charging_infrastructure").resolve()
DATASET_CFG = config.datasets()["charging_infrastructure"]


def run_tracbev():
    data_dict = get_data()

    run_tracbev_potential(data_dict)


def run_use_cases(data_dict):
    hpc(data_dict["hpc_positions"], data_dict)
    public(data_dict["public_positions"], data_dict["poi_cluster"], data_dict)
    # uc.work(data_dict["landuse"], data_dict["work_dict"], data_dict)
    # uc.home(data_dict["housing_data"], data_dict, 0, 0)


def run_tracbev_potential(data_dict):
    bounds = data_dict["boundaries"]

    for mv_grid_id in data_dict["regions"].mv_grid_id:
        region = bounds.loc[bounds.bus_id == mv_grid_id].geom

        data_dict.update({"region": region, "key": mv_grid_id})
        # Start Use Cases
        run_use_cases(data_dict)


def get_data() -> dict[gpd.GeoDataFrame]:
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

    return data_dict
