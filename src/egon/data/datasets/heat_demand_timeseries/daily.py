from datetime import datetime
import os

from sqlalchemy import Column, Float, Integer, Text
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db, config
from egon.data.datasets.scenario_parameters import get_sector_parameters
import egon.data.datasets.era5 as era


from math import ceil


Base = declarative_base()


class EgonMapZensusClimateZones(Base):
    __tablename__ = "egon_map_zensus_climate_zones"
    __table_args__ = {"schema": "boundaries"}

    zensus_population_id = Column(Integer, primary_key=True)
    climate_zone = Column(Text)


class EgonDailyHeatDemandPerClimateZone(Base):
    __tablename__ = "egon_daily_heat_demand_per_climate_zone"
    __table_args__ = {"schema": "demand"}

    climate_zone = Column(Text, primary_key=True)
    day_of_year = Column(Integer, primary_key=True)
    temperature_class = Column(Integer)
    daily_demand_share = Column(Float(53))


def temperature_classes():
    return {
        -20: 1,
        -19: 1,
        -18: 1,
        -17: 1,
        -16: 1,
        -15: 1,
        -14: 2,
        -13: 2,
        -12: 2,
        -11: 2,
        -10: 2,
        -9: 3,
        -8: 3,
        -7: 3,
        -6: 3,
        -5: 3,
        -4: 4,
        -3: 4,
        -2: 4,
        -1: 4,
        0: 4,
        1: 5,
        2: 5,
        3: 5,
        4: 5,
        5: 5,
        6: 6,
        7: 6,
        8: 6,
        9: 6,
        10: 6,
        11: 7,
        12: 7,
        13: 7,
        14: 7,
        15: 7,
        16: 8,
        17: 8,
        18: 8,
        19: 8,
        20: 8,
        21: 9,
        22: 9,
        23: 9,
        24: 9,
        25: 9,
        26: 10,
        27: 10,
        28: 10,
        29: 10,
        30: 10,
        31: 10,
        32: 10,
        33: 10,
        34: 10,
        35: 10,
        36: 10,
        37: 10,
        38: 10,
        39: 10,
        40: 10,
    }


def map_climate_zones_to_zensus():
    """Geospatial join of zensus cells and climate zones

    Returns
    -------
    None.

    """
    # Drop old table and create new one
    engine = db.engine()
    EgonMapZensusClimateZones.__table__.drop(bind=engine, checkfirst=True)
    EgonMapZensusClimateZones.__table__.create(bind=engine, checkfirst=True)

    # Read in file containing climate zones
    temperature_zones = gpd.read_file(
        os.path.join(
            os.getcwd(),
            "data_bundle_egon_data",
            "climate_zones_Germany",
            "TRY_Climate_Zone",
            "Climate_Zone.shp",
        )
    ).set_index("Station")

    # Import census cells and their centroids
    census_cells = db.select_geodataframe(
        f"""
        SELECT id as zensus_population_id, geom_point as geom
        FROM society.destatis_zensus_population_per_ha_inside_germany
        """,
        index_col="zensus_population_id",
        epsg=4326,
    )

    # Join climate zones and census cells
    join = (
        census_cells.sjoin(temperature_zones)
        .rename({"index_right": "climate_zone"}, axis="columns")
        .climate_zone
    )

    # Drop duplicates (some climate zones are overlapping)
    join = join[~join.index.duplicated(keep="first")]

    # Insert resulting dataframe to SQL table
    join.to_sql(
        EgonMapZensusClimateZones.__table__.name,
        schema=EgonMapZensusClimateZones.__table__.schema,
        con=db.engine(),
        if_exists="replace",
    )


def daily_demand_shares_per_climate_zone():
    """Calculates shares of heat demand per day for each climate zone

    Returns
    -------
    None.

    """
    # Drop old table and create new one
    engine = db.engine()
    EgonDailyHeatDemandPerClimateZone.__table__.drop(
        bind=engine, checkfirst=True
    )
    EgonDailyHeatDemandPerClimateZone.__table__.create(
        bind=engine, checkfirst=True
    )

    # Get temperature profiles of all TRY Climate Zones 2011
    temp_profile = temperature_profile_extract()

    # Calulate daily demand shares
    h = h_value(temp_profile)

    # Normalize data to sum()=1
    daily_demand_shares = h.resample("d").sum() / h.sum()

    # Extract temperature class for each day and climate zone
    temperature_classes = temp_interval(temp_profile).resample("D").max()

    # Initilize dataframe
    df = pd.DataFrame(
        columns=[
            "climate_zone",
            "day_of_year",
            "temperature_class",
            "daily_demand_share",
        ]
    )

    # Insert data into dataframe
    for index, row in daily_demand_shares.transpose().iterrows():
        df = pd.concat(
            [
                df,
                pd.DataFrame(
                    data={
                        "climate_zone": index,
                        "day_of_year": row.index.day_of_year,
                        "daily_demand_share": row.values,
                        "temperature_class": temperature_classes[index][
                            row.index
                        ],
                    }
                ),
            ],
            ignore_index=True,
        )

    # Insert dataframe to SQL table
    df.to_sql(
        EgonDailyHeatDemandPerClimateZone.__table__.name,
        schema=EgonDailyHeatDemandPerClimateZone.__table__.schema,
        con=db.engine(),
        if_exists="replace",
        index=False,
    )


class IdpProfiles:
    def __init__(self, df_index, **kwargs):
        self.df = pd.DataFrame(index=df_index)

        self.temperature = kwargs.get("temperature")

    def get_temperature_interval(self, how="geometric_series"):
        """Appoints the corresponding temperature interval to each temperature
        in the temperature vector.
        """
        self.df["temperature"] = self.temperature.values

        temperature = (
            self.df["temperature"]
            .resample("D")
            .mean()
            .reindex(self.df.index)
            .fillna(method="ffill")
            .fillna(method="bfill")
        )

        if how == "geometric_series":
            temperature_mean = (
                temperature
                + 0.5 * np.roll(temperature, 24)
                + 0.25 * np.roll(temperature, 48)
                + 0.125 * np.roll(temperature, 72)
            ) / 1.875
        elif how == "mean":
            temperature_mean = temperature

        else:
            temperature_mean = None

        self.df["temperature_geo"] = temperature_mean

        temperature_rounded = []

        for i in self.df["temperature_geo"]:
            temperature_rounded.append(ceil(i))

        intervals = temperature_classes()

        temperature_interval = []
        for i in temperature_rounded:
            temperature_interval.append(intervals[i])

        self.df["temperature_interval"] = temperature_interval

        return self.df


def temperature_profile_extract():
    """
    Description: Extract temperature data from atlite
    Returns
    -------
    temperature_profile : pandas.DataFrame
        Temperatur profile of all TRY Climate Zones 2011

    """

    cutout = era.import_cutout(boundary="Germany")

    coordinates_path = os.path.join(
        os.getcwd(),
        "data_bundle_egon_data",
        "climate_zones_Germany",
        "TRY_Climate_Zone",
    )
    station_location = pd.read_csv(
        os.path.join(coordinates_path, "station_coordinates.csv")
    )

    weather_cells = db.select_geodataframe(
        """
        SELECT geom FROM supply.egon_era5_weather_cells
        """,
        epsg=4326,
    )

    gdf = gpd.GeoDataFrame(
        station_location,
        geometry=gpd.points_from_xy(
            station_location.Longitude, station_location.Latitude
        ),
    )

    selected_weather_cells = gpd.sjoin(weather_cells, gdf).set_index("Station")

    temperature_profile = cutout.temperature(
        shapes=selected_weather_cells.geom.values,
        index=selected_weather_cells.index,
    ).to_pandas()

    return temperature_profile


def temp_interval(temp_profile):
    """
    Description: Create Dataframe with temperature data for TRY Climate Zones

    temp_profile: pandas.DataFrame
        temperature profiles of all TRY Climate Zones 2011
    Returns
    -------
    temperature_interval : pandas.DataFrame
        Hourly temperature intrerval of all 15 TRY Climate station#s temperature profile

    """
    #ToDo: Make this function scenario friendly
    scenario = config.settings()["egon-data"]["--scenarios"][0]
    year = get_sector_parameters("global", scenario)["weather_year"]

    index = pd.date_range(datetime(year, 1, 1, 0), periods=8760, freq="H")
    temperature_interval = pd.DataFrame()

    for x in range(len(temp_profile.columns)):
        name_station = temp_profile.columns[x]
        idp_this_station = IdpProfiles(
            index, temperature=temp_profile[temp_profile.columns[x]]
        ).get_temperature_interval(how="geometric_series")
        temperature_interval[name_station] = idp_this_station[
            "temperature_interval"
        ]

    return temperature_interval


def h_value(temp_profile):
    """
    Description: Assignment of daily demand scaling factor to each day of all TRY Climate Zones

    temp_profile: pandas.DataFrame
        temperature profiles of all TRY Climate Zones 2011
    Returns
    -------
    h : pandas.DataFrame
        Hourly factor values for each station corresponding to the temperature profile.
        Extracted from demandlib.

    """
    #ToDo: Make this function scenario friendly
    scenario = config.settings()["egon-data"]["--scenarios"][0]

    year = get_sector_parameters("global", scenario)["weather_year"]
    index = pd.date_range(datetime(year, 1, 1, 0), periods=8760, freq="H")

    a = 3.0469695

    b = -37.1833141

    c = 5.6727847

    d = 0.1163157

    temperature_profile_res = (
        temp_profile.resample("D")
        .mean()
        .reindex(index)
        .fillna(method="ffill")
        .fillna(method="bfill")
    )

    temp_profile_geom = (
        (
            temperature_profile_res.transpose()
            + 0.5 * np.roll(temperature_profile_res.transpose(), 24, axis=1)
            + 0.25 * np.roll(temperature_profile_res.transpose(), 48, axis=1)
            + 0.125 * np.roll(temperature_profile_res.transpose(), 72, axis=1)
        )
        / 1.875
    ).transpose()

    h = a / (1 + (b / (temp_profile_geom - 40)) ** c) + d

    return h
