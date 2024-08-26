"""
Read data from DB and download.
"""
from pathlib import Path

from loguru import logger
import geopandas as gpd
import pandas as pd

from egon.data import config
from egon.data.db import select_geodataframe

DATASET_CFG = config.datasets()["mobility_hgv"]
WORKING_DIR = Path(".", "heavy_duty_transport").resolve()
TESTMODE_OFF = (
    config.settings()["egon-data"]["--dataset-boundary"] == "Everything"
)


def get_data():
    """
    Load all necessary data.
    """
    return boundary_gdf(), bast_gdf(), nuts3_gdf()


def boundary_gdf():
    """
    Get outer boundary from database.
    """
    srid = DATASET_CFG["tables"]["srid"]

    gdf = select_geodataframe(
        """
        SELECT id, geometry FROM boundaries.vg250_lan
        ORDER BY id
        """,
        geom_col="geometry",
        index_col="id",
    ).to_crs(epsg=srid)

    return gdf.dissolve()


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
        delimiter=r",",
        decimal=r",",
        thousands=r".",
        encoding="ISO-8859-1",
        usecols=relevant_columns,
    )

    init_srid = sources["BAST"]["srid"]
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


def nuts3_gdf():
    """Read in NUTS3 geo shapes."""
    srid = DATASET_CFG["tables"]["srid"]
    sql = """
        SELECT nuts as nuts3, geometry FROM boundaries.vg250_krs
        WHERE gf = 4
        ORDER BY nuts
        """

    gdf = select_geodataframe(
        sql, geom_col="geometry", index_col="nuts3"
    ).to_crs(epsg=srid)

    gdf["area"] = gdf.geometry.area

    logger.debug("Read in NUTS 3 districts.")

    return gdf
