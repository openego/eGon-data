from loguru import logger
import geopandas as gpd
import pandas as pd

from egon.data.datasets.emobility.heavy_duty_transport import (
    DATASET_CFG,
    WORKING_DIR,
)
from egon.data.db import select_geodataframe


def get_data():
    """
    Load all necessary data.
    """
    return germany_gdf(), bast_gdf(), grid_districts_gdf()


def germany_gdf():
    """
    Read in German Border from geo.json file.
    """
    sources = DATASET_CFG["original_data"]["sources"]
    srid = DATASET_CFG["tables"]["srid"]

    gdf = gpd.read_file(sources["germany"]["url"]).to_crs(epsg=srid)

    logger.debug("Downloaded germany GeoJSON.")

    return gdf


def bast_gdf():
    """
    Reads BAST data.
    """
    sources = DATASET_CFG["original_data"]["sources"]
    file = sources["BAST"]["file"]

    path = WORKING_DIR / file
    relevant_columns = sources["BAST"]["relevant_columns"]

    df = pd.read_csv(
        path,
        delimiter=r";",
        decimal=r",",
        thousands=r".",
        encoding="ISO-8859-1",
        usecols=relevant_columns,
    )

    init_srid = sources["BAST"]["file"]
    final_srid = DATASET_CFG["tables"]["srid"]

    gdf = gpd.GeoDataFrame(
        df[relevant_columns[0]],
        geometry=gpd.points_from_xy(
            df[relevant_columns[1]],
            df[relevant_columns[2]],
            crs=f"EPSG:{init_srid}",
        ),
    ).to_crs(epsg=final_srid)

    logger.debug("Read in BAST data.")

    return gdf


def grid_districts_gdf():
    sql = """
        SELECT bus_id, geom FROM grid.egon_mv_grid_district
        ORDER BY bus_id
        """

    gdf = select_geodataframe(sql, geom_col="geom", index_col="bus_id")

    logger.debug("Read in MV grid districts.")

    return gdf
