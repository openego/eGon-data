"""
Central module containing all code dealing with importing era5 weather data.
"""
from pathlib import Path
import os

from geoalchemy2 import Geometry
from sqlalchemy import ARRAY, Column, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import atlite
import geopandas as gpd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.scenario_parameters import get_sector_parameters
import egon.data.config

# will be later imported from another file ###
Base = declarative_base()


class WeatherData(Dataset):
    """
    Download weather data from ERA5 using atlite

    This dataset downloads weather data for the selected representative weather year.
    This is done by applying functions from the atlite-tool.The downloaded wetaher data is stored into
    files within the subfolder 'cutouts'.


    *Dependencies*
      * :py:class:`ScenarioParameters <egon.data.datasets.scenario_parameters.ScenarioParameters>`
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`
      * :py:func:`Setup <egon.data.datasets.database.setup>`

    *Resulting tables*
      * :py:class:`supply.egon_era5_weather_cells <egon.data.datasets.era5.EgonEra5Cells>` is created and filled
      * :py:class:`supply.egon_era5_renewable_feedin <egon.data.datasets.era5.EgonRenewableFeedIn>` is created

    """

    #:
    name: str = "Era5"
    #:
    version: str = "0.0.3"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=({create_tables, download_era5}, insert_weather_cells),
        )


class EgonEra5Cells(Base):
    """
    Class definition of table supply.egon_era5_weather_cells.

    """
    __tablename__ = "egon_era5_weather_cells"
    __table_args__ = {"schema": "supply"}
    w_id = Column(Integer, primary_key=True)
    geom = Column(Geometry("POLYGON", 4326))
    geom_point = Column(Geometry("POINT", 4326))


class EgonRenewableFeedIn(Base):
    """
    Class definition of table supply.egon_era5_renewable_feedin.

    """
    __tablename__ = "egon_era5_renewable_feedin"
    __table_args__ = {"schema": "supply"}
    w_id = Column(Integer, primary_key=True)
    weather_year = Column(Integer, primary_key=True)
    carrier = Column(String, primary_key=True)
    feedin = Column(ARRAY(Float()))


def create_tables():
    db.execute_sql("CREATE SCHEMA IF NOT EXISTS supply;")
    engine = db.engine()
    db.execute_sql(
        f"""
        DROP TABLE IF EXISTS {EgonEra5Cells.__table__.schema}.{EgonEra5Cells.__table__.name} CASCADE;
        """
    )
    EgonEra5Cells.__table__.create(bind=engine, checkfirst=True)
    EgonRenewableFeedIn.__table__.drop(bind=engine, checkfirst=True)
    EgonRenewableFeedIn.__table__.create(bind=engine, checkfirst=True)


def import_cutout(boundary="Europe"):
    """Import weather data from cutout

    Returns
    -------
    cutout : atlite.cutout.Cutout
        Weather data stored in cutout

    """
    for scn in set(egon.data.config.settings()["egon-data"]["--scenarios"]):
        weather_year = get_sector_parameters("global", scn)["weather_year"]

        if boundary == "Europe":
            xs = slice(-12.0, 35.1)
            ys = slice(72.0, 33.0)

        elif boundary == "Germany":
            geom_de = (
                gpd.read_postgis(
                    "SELECT geometry as geom FROM boundaries.vg250_sta_bbox",
                    db.engine(),
                )
                .to_crs(4326)
                .geom
            )
            xs = slice(geom_de.bounds.minx[0], geom_de.bounds.maxx[0])
            ys = slice(geom_de.bounds.miny[0], geom_de.bounds.maxy[0])

        elif boundary == "Germany-offshore":
            xs = slice(5.5, 14.5)
            ys = slice(55.5, 53.5)

        else:
            print(
                f"Boundary {boundary} not defined. "
                "Choose either 'Europe' or 'Germany'"
            )

        directory = (
            Path(".")
            / (
                egon.data.config.datasets()["era5_weather_data"]["targets"][
                    "weather_data"
                ]["path"]
            )
            / f"{boundary.lower()}-{str(weather_year)}-era5.nc"
        )

        return atlite.Cutout(
            path=directory.absolute(),
            module="era5",
            x=xs,
            y=ys,
            years=slice(weather_year, weather_year),
        )


def download_era5():
    """Download weather data from era5

    Returns
    -------
    None.

    """

    directory = Path(".") / (
        egon.data.config.datasets()["era5_weather_data"]["targets"][
            "weather_data"
        ]["path"]
    )

    if not os.path.exists(directory):
        os.mkdir(directory)

    cutout = import_cutout()

    if not cutout.prepared:
        cutout.prepare()

    cutout = import_cutout("Germany")

    if not cutout.prepared:
        cutout.prepare()

    cutout = import_cutout("Germany-offshore")

    if not cutout.prepared:
        cutout.prepare()


def insert_weather_cells():
    """Insert weather cells from era5 into database table

    Returns
    -------
    None.

    """
    cfg = egon.data.config.datasets()["era5_weather_data"]

    db.execute_sql(
        f"""
        DELETE FROM {cfg['targets']['weather_cells']['schema']}.
        {cfg['targets']['weather_cells']['table']}
        """
    )

    cutout = import_cutout()

    df = gpd.GeoDataFrame(
        {"geom": cutout.grid.geometry}, geometry="geom", crs=4326
    )

    df.to_postgis(
        cfg["targets"]["weather_cells"]["table"],
        schema=cfg["targets"]["weather_cells"]["schema"],
        con=db.engine(),
        if_exists="append",
    )

    db.execute_sql(
        f"""UPDATE {cfg['targets']['weather_cells']['schema']}.
        {cfg['targets']['weather_cells']['table']}
        SET geom_point=ST_Centroid(geom);"""
    )
