"""
Read data from DB and downloads
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
    Read in German Border from geo.json file.
    """
    sources = DATASET_CFG["original_data"]["sources"]
    srid = DATASET_CFG["tables"]["srid"]

    if TESTMODE_OFF:
        gdf = gpd.read_file(sources["germany"]["url"]).to_crs(epsg=srid)

        logger.debug("Downloaded germany GeoJSON.")
    else:
        path = (
            WORKING_DIR
            / "_".join(sources["NUTS"]["file"].split(".")[:-1])
            / sources["NUTS"]["shp_file"]
        )

        gdf = gpd.read_file(path).to_crs(epsg=srid)

        gdf = gdf.loc[gdf.NUTS_CODE == sources["NUTS"]["NUTS_CODE"]].dissolve()

        logger.debug("Loaded SH shape file.")

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
