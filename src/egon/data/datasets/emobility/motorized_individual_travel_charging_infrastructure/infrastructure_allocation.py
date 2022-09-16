from __future__ import annotations

from pathlib import Path

import geopandas as gpd

from egon.data import config, db

WORKING_DIR = Path(".", "charging_infrastructure").resolve()
DATASET_CFG = config.datasets()["charging_infrastructure"]


def get_data() -> dict[gpd.GeoDataFrame]:
    tracbev_cfg = DATASET_CFG["original_data"]["sources"]["tracbev"]
    files = tracbev_cfg["files_to_use"]
    srid = tracbev_cfg["srid"]

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

    # TODO: get zensus housing data from DB instead of gpkg?

    return data_dict
