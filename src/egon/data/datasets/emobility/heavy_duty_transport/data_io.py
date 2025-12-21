"""
Read data from DB and download.
"""

from pathlib import Path

from loguru import logger
import geopandas as gpd
import pandas as pd

from egon.data.db import select_geodataframe

from egon.data.datasets import load_sources_and_targets


def get_data():
    """
    Load all necessary data.
    """
    return boundary_gdf(), bast_gdf(), nuts3_gdf()


def boundary_gdf():
    """
    Get outer boundary from database.
    """
    #Local Import for SRID (Constant from Class)
    from egon.data.datasets.emobility.heavy_duty_transport import HeavyDutyTransport
    srid = HeavyDutyTransport.srid

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
    sources, targets = load_sources_and_targets("HeavyDutyTransport")
    
    # Local Import for Constants (Columns, SRID)
    from egon.data.datasets.emobility.heavy_duty_transport import HeavyDutyTransport

    # Get file path from targets
    path = Path(targets.files["BAST_download"])
    
    # Get constants from Class
    relevant_columns = HeavyDutyTransport.bast_relevant_columns
    init_srid = HeavyDutyTransport.bast_srid
    final_srid = HeavyDutyTransport.srid

    df = pd.read_csv(
    path,
    sep=r"[,;]",      
    engine="python",
    decimal=r",",
    thousands=r".",
    encoding="ISO-8859-1",
    usecols=relevant_columns,
)

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
    # Local Import for SRID
    from egon.data.datasets.emobility.heavy_duty_transport import HeavyDutyTransport
    srid = HeavyDutyTransport.srid
    
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