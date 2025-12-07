"""
Read data from DB and download.
"""

from pathlib import Path

from loguru import logger
import geopandas as gpd
import pandas as pd

from egon.data.db import select_geodataframe


def get_data():
    """
    Load all necessary data.
    """
    return boundary_gdf(), bast_gdf(), nuts3_gdf()


def boundary_gdf():
    """
    Get outer boundary from database.
    """
    srid = 3035  # From YML

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
    from egon.data.datasets.emobility.heavy_duty_transport import HeavyDutyTransport

    # Path from HeavyDutyTransport class
    path = Path(HeavyDutyTransport.targets.files["BAST_download"])
    
    # from YML
    relevant_columns = ["DTV_SV_MobisSo_Q", "Koor_WGS84_E", "Koor_WGS84_N"]

    df = pd.read_csv(
        path,
        delimiter=r",",
        decimal=r",",
        thousands=r".",
        encoding="ISO-8859-1",
        usecols=relevant_columns,
    )

    init_srid = 4326 # From YML
    final_srid = 3035 # From YML

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
    from egon.data.datasets.emobility.heavy_duty_transport import HeavyDutyTransport

    srid = 3035 # From YML
    
    source_table = HeavyDutyTransport.sources.tables["vg250_krs"]
    
    sql = f"""
        SELECT nuts as nuts3, geometry FROM {source_table}
        WHERE gf = 4
        ORDER BY nuts
        """

    gdf = select_geodataframe(
        sql, geom_col="geometry", index_col="nuts3"
    ).to_crs(epsg=srid)

    gdf["area"] = gdf.geometry.area

    logger.debug("Read in NUTS 3 districts.")

    return gdf