from datetime import datetime
import os

from sqlalchemy import ARRAY, Column, Float, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd
import pandas.io.sql as sqlio


from egon.data import db
import egon.data.datasets.era5 as era

try:
    from disaggregator import temporal
except ImportError as e:
    pass

from math import ceil

from egon.data.datasets import Dataset
import egon


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
    heat_demand_share = Column(Float(53))


class EgonHeatTimeseries(Base):
    __tablename__ = "heat_timeseries_selected_profiles"
    __table_args__ = {"schema": "demand"}
    zensus_population_id = Column(Integer, primary_key=True)
    building_id = Column(Integer, primary_key=True)
    selected_idp_profiles = Column(ARRAY(Integer))


class EgonTimeseriesDistrictHeating(Base):
    __tablename__ = "egon_timeseries_district_heating_new"
    __table_args__ = {"schema": "demand"}
    area_id = Column(Integer, primary_key=True)
    scenario = Column(Text, primary_key=True)
    dist_aggregated_mw = Column(ARRAY(Float(53)))


class EgonEtragoTimeseriesIndividualHeating(Base):
    __tablename__ = "egon_etrago_timeseries_individual_heating"
    __table_args__ = {"schema": "demand"}
    bus_id = Column(Integer, primary_key=True)
    scenario = Column(Text, primary_key=True)
    dist_aggregated_mw = Column(ARRAY(Float(53)))


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

    # Insert resulting dataframe to SQL table
    join.to_sql(
        EgonMapZensusClimateZones.__table__.name,
        schema=EgonMapZensusClimateZones.__table__.schema,
        con=db.engine(),
        if_exists="replace",
    )


def daily_demand_shares_per_climate_zone():
    """Calculates shares of heat demand per day for each cliamte zone

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

    # Calulate daily demand shares
    h = h_value()

    # Normalize data to sum()=1
    daily_demand_shares = h.resample("d").sum() / h.sum()

    # Extract temperature class for each day and climate zone
    temperature_classes = temp_interval().resample("D").max()

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

        df = df.append(
            pd.DataFrame(
                data={
                    "climate_zone": index,
                    "day_of_year": row.index.day_of_year,
                    "daily_demand_share": row.values,
                    "temperature_class": temperature_classes[index][row.index],
                }
            )
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
        index = pd.date_range(datetime(2011, 1, 1, 0), periods=8760, freq="H")

        self.df = pd.DataFrame(index=df_index)

        self.temperature = kwargs.get("temperature")

    def get_temperature_interval(self, how="geometric_series"):
        index = pd.date_range(datetime(2011, 1, 1, 0), periods=8760, freq="H")
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


def temp_interval():
    """
    Description: Create Dataframe with temperature data for TRY Climate Zones
    Returns
    -------
    temperature_interval : pandas.DataFrame
        Hourly temperature intrerval of all 15 TRY Climate station#s temperature profile

    """
    index = pd.date_range(datetime(2011, 1, 1, 0), periods=8760, freq="H")
    temperature_interval = pd.DataFrame()
    temp_profile = temperature_profile_extract()

    for x in range(len(temp_profile.columns)):
        name_station = temp_profile.columns[x]
        idp_this_station = IdpProfiles(
            index, temperature=temp_profile[temp_profile.columns[x]]
        ).get_temperature_interval(how="geometric_series")
        temperature_interval[name_station] = idp_this_station[
            "temperature_interval"
        ]

    return temperature_interval


def idp_pool_generator():
    """
    Description: Create List of Dataframes for each temperature class for each household stock

     Returns
     -------
     TYPE list
         List of dataframes with each element representing a dataframe
         for every combination of household stock and temperature class

    """
    path = os.path.join(
        os.getcwd(),
        "data_bundle_egon_data",
        "household_heat_demand_profiles",
        "household_heat_demand_profiles.hdf5",
    )
    index = pd.date_range(datetime(2011, 1, 1, 0), periods=8760, freq="H")

    sfh = pd.read_hdf(path, key="SFH")
    mfh = pd.read_hdf(path, key="MFH")
    temp = pd.read_hdf(path, key="temperature")

    globals()["luebeck_sfh"] = sfh[sfh.filter(like="Luebeck").columns]
    globals()["luebeck_mfh"] = mfh[mfh.filter(like="Luebeck").columns]

    globals()["kassel_sfh"] = sfh[sfh.filter(like="Kassel").columns]
    globals()["kassel_mfh"] = mfh[mfh.filter(like="Kassel").columns]

    globals()["wuerzburg_sfh"] = sfh[sfh.filter(like="Wuerzburg").columns]
    globals()["wuerzburg_mfh"] = mfh[mfh.filter(like="Wuerzburg").columns]

    globals()["chemnitz_sfh"] = sfh[sfh.filter(like="Chemnitz").columns]
    globals()["chemnitz_mfh"] = mfh[mfh.filter(like="Chemnitz").columns]

    temp_daily = pd.DataFrame()
    for column in temp.columns:
        temp_current = (
            temp[column]
            .resample("D")
            .mean()
            .reindex(temp.index)
            .fillna(method="ffill")
            .fillna(method="bfill")
        )
        temp_current_geom = temp_current
        temp_daily = pd.concat([temp_daily, temp_current_geom], axis=1)

    def round_temperature(station):
        """
        Description: Create dataframe to assign temperature class to each day of TRY climate zones

        Parameters
        ----------
        station : str
            Name of the location

        Returns
        -------
        temp_class : pandas.DataFrame
            Each day assignd to their respective temperature class

        """
        intervals = temperature_classes()
        temperature_rounded = []
        for i in temp_daily.loc[:, station]:
            temperature_rounded.append(ceil(i))
        temperature_interval = []
        for i in temperature_rounded:
            temperature_interval.append(intervals[i])
        temp_class_dic = {f"Class_{station}": temperature_interval}
        temp_class = pd.DataFrame.from_dict(temp_class_dic)
        return temp_class

    temp_class_luebeck = round_temperature("Luebeck")
    temp_class_kassel = round_temperature("Kassel")
    temp_class_wuerzburg = round_temperature("Wuerzburg")
    temp_class_chemnitz = round_temperature("Chemnitz")
    temp_class = pd.concat(
        [
            temp_class_luebeck,
            temp_class_kassel,
            temp_class_wuerzburg,
            temp_class_chemnitz,
        ],
        axis=1,
    )
    temp_class.set_index(index, inplace=True)

    def unique_classes(station):
        """

        Returns
        -------
        classes : list
            Collection of temperature classes for each location

        """
        classes = []
        for x in temp_class[f"Class_{station}"]:
            if x not in classes:
                classes.append(x)
        classes.sort()
        return classes

    globals()["luebeck_classes"] = unique_classes("Luebeck")
    globals()["kassel_classes"] = unique_classes("Kassel")
    globals()["wuerzburg_classes"] = unique_classes("Wuerzburg")
    globals()["chemnitz_classes"] = unique_classes("Chemnitz")

    stock = ["MFH", "SFH"]
    class_list = [2, 3, 4, 5, 6, 7, 8, 9, 10]
    for s in stock:
        for m in class_list:
            globals()[f"idp_collection_class_{m}_{s}"] = pd.DataFrame(
                index=range(24)
            )

    def splitter(station, household_stock):
        """


        Parameters
        ----------
        station : str
            Name of the location
        household_stock : str
            SFH or MFH

        Returns
        -------
        None.

        """
        this_classes = globals()["{}_classes".format(station.lower())]
        for classes in this_classes:
            this_itteration = globals()[
                "{}_{}".format(station.lower(), household_stock.lower())
            ].loc[temp_class["Class_{}".format(station)] == classes, :]
            days = list(range(int(len(this_itteration) / 24)))
            for day in days:
                this_day = this_itteration[day * 24 : (day + 1) * 24]
                this_day = this_day.reset_index(drop=True)
                globals()[
                    f"idp_collection_class_{classes}_{household_stock}"
                ] = pd.concat(
                    [
                        globals()[
                            f"idp_collection_class_{classes}_{household_stock}"
                        ],
                        this_day,
                    ],
                    axis=1,
                    ignore_index=True,
                )

    splitter("Luebeck", "SFH")
    splitter("Kassel", "SFH")
    splitter("Wuerzburg", "SFH")
    splitter("Chemnitz", "SFH")
    splitter("Luebeck", "MFH")
    splitter("Kassel", "MFH")
    splitter("Chemnitz", "MFH")

    def pool_normalize(x):
        """


        Parameters
        ----------
        x : pandas.Series
            24-hour profiles of IDP pool

        Returns
        -------
        TYPE : pandas.Series
            Normalized to their daily total

        """
        if x.sum() != 0:
            c = x.sum()
            return x / c
        else:
            return x

    stock = ["MFH", "SFH"]
    class_list = [2, 3, 4, 5, 6, 7, 8, 9, 10]
    for s in stock:
        for m in class_list:
            df_name = globals()[f"idp_collection_class_{m}_{s}"]
            globals()[f"idp_collection_class_{m}_{s}_norm"] = df_name.apply(
                pool_normalize
            )

    return [
        idp_collection_class_2_SFH_norm,
        idp_collection_class_3_SFH_norm,
        idp_collection_class_4_SFH_norm,
        idp_collection_class_5_SFH_norm,
        idp_collection_class_6_SFH_norm,
        idp_collection_class_7_SFH_norm,
        idp_collection_class_8_SFH_norm,
        idp_collection_class_9_SFH_norm,
        idp_collection_class_10_SFH_norm,
        idp_collection_class_2_MFH_norm,
        idp_collection_class_3_MFH_norm,
        idp_collection_class_4_MFH_norm,
        idp_collection_class_5_MFH_norm,
        idp_collection_class_6_MFH_norm,
        idp_collection_class_7_MFH_norm,
        idp_collection_class_8_MFH_norm,
        idp_collection_class_9_MFH_norm,
        idp_collection_class_10_MFH_norm,
    ]


def idp_df_generator():
    """
    Description: Create dataframe with all temprature classes, 24hr. profiles and household stock

    Returns
    -------
    idp_df : pandas.DataFrame
        All IDP pool as classified as per household stock and temperature class

    """
    idp_list = idp_pool_generator()
    stock = ["MFH", "SFH"]
    class_list = [2, 3, 4, 5, 6, 7, 8, 9, 10]
    idp_df = pd.DataFrame(columns=["idp", "house", "temperature_class"])
    for s in stock:
        for m in class_list:

            if s == "SFH":
                i = class_list.index(m)
            if s == "MFH":
                i = class_list.index(m) + 9
            current_pool = idp_list[i]
            idp_df = idp_df.append(
                pd.DataFrame(
                    data={
                        "idp": current_pool.transpose().values.tolist(),
                        "house": s,
                        "temperature_class": m,
                    }
                )
            )
    idp_df = idp_df.reset_index(drop=True)

    idp_df.to_sql(
        "heat_idp_pool",
        con=db.engine(),
        schema="demand",
        if_exists="replace",
        index=True,
        dtype={
            "index": Integer(),
            "idp": ARRAY(Float()),
            "house": String(),
            "temperature_class": Integer(),
        },
    )

    idp_df["idp"] = idp_df.idp.apply(lambda x: np.array(x))

    idp_df.idp = idp_df.idp.apply(lambda x: x.astype(np.float32))

    return idp_df


def psycop_df_AF(table_name):
    """
    Description: Read tables from database into pandas dataframe
    Parameters
    ----------
    table_name : str
        Name of the database table

    Returns
    -------
    data : pandas.DataFrame
        Imported database tables


    """
    conn = db.engine()
    sql = "SELECT * FROM {}".format(table_name)
    data = sqlio.read_sql_query(sql, conn)
    conn = None
    return data


def psycop_gdf_AF(table_name, geom_column="geom"):
    """

    Description: Read tables from database into geopandas dataframe
    Parameters
    ----------
    table_name : str
        Name of the database table
    geom_column : str, optional
        Column name with geometry. The default is 'geom'.

    Returns
    -------
    data : Tandas.DataFrame
        Imported database tables

    """
    conn = db.engine()
    sql = "SELECT * FROM {}".format(table_name)
    data = gpd.read_postgis(sql, conn, geom_column)
    conn = None
    return data


def annual_demand_generator():
    """

    Description: Create dataframe with annual demand and household count for each zensus cell
    Returns
    -------
    demand_count: pandas.DataFrame
        Annual demand of all zensus cell with MFH and SFH count and
        respective associated Station

    """

    scenario = "eGon2035"
    demand_zone = db.select_dataframe(
        f"""
        SELECT a.demand, a.zensus_population_id, a.scenario, c.climate_zone
        FROM demand.egon_peta_heat a
        JOIN boundaries.egon_map_zensus_climate_zones c
        ON a.zensus_population_id = c.zensus_population_id
        WHERE a.sector = 'residential'
        AND a.scenario = '{scenario}'
        """,
        index_col="zensus_population_id",
    )

    house_count_MFH = db.select_dataframe(
        """
        
        SELECT cell_id as zensus_population_id, COUNT(*) as number FROM 
        (
        SELECT cell_id, COUNT(*), building_id
        FROM demand.egon_household_electricity_profile_of_buildings
        GROUP BY (cell_id, building_id)
        ) a
        
        WHERE a.count >1
        GROUP BY cell_id
        """,
        index_col="zensus_population_id",
    )

    house_count_SFH = db.select_dataframe(
        """
        
        SELECT cell_id as zensus_population_id, COUNT(*) as number FROM 
        (
        SELECT cell_id, COUNT(*), building_id
        FROM demand.egon_household_electricity_profile_of_buildings
        GROUP BY (cell_id, building_id)
        ) a 
        WHERE a.count = 1
        GROUP BY cell_id
        """,
        index_col="zensus_population_id",
    )

    demand_zone["SFH"] = house_count_SFH.number
    demand_zone["MFH"] = house_count_MFH.number

    demand_zone["SFH"].fillna(0, inplace=True)
    demand_zone["MFH"].fillna(0, inplace=True)

    return demand_zone


def profile_selector():
    """

    Random assignment of intray-day profiles to each day based on their temeprature class
    and household stock count

    Returns
    -------
    None.

    """
    start_profile_selector = datetime.now()

    # Drop old table and re-create it
    engine = db.engine()
    EgonHeatTimeseries.__table__.drop(bind=engine, checkfirst=True)
    EgonHeatTimeseries.__table__.create(bind=engine, checkfirst=True)

    # Select all intra-day-profiles
    idp_df = db.select_dataframe(
        """
        SELECT index, house, temperature_class
        FROM demand.heat_idp_pool
        """,
        index_col="index",
    )

    # Select daily heat demand shares per climate zone from table
    temperature_classes = db.select_dataframe(
        """
        SELECT climate_zone, day_of_year, temperature_class
        FROM demand.egon_daily_heat_demand_per_climate_zone
        """
    )

    # Calculate annual heat demand per census cell
    annual_demand = annual_demand_generator()

    # Count number of SFH and MFH per climate zone
    houses_per_climate_zone = (
        annual_demand.groupby("climate_zone")[["SFH", "MFH"]].sum().astype(int)
    )

    # Set random seed to make code reproducable
    np.random.seed(
        seed=egon.data.config.settings()["egon-data"]["--random-seed"]
    )

    for station in houses_per_climate_zone.index:

        result_SFH = pd.DataFrame(columns=range(1, 366))
        result_MFH = pd.DataFrame(columns=range(1, 366))

        # Randomly select individual daily demand profile for selected climate zone
        for day in range(1, 366):
            t_class = temperature_classes.loc[
                (temperature_classes.climate_zone == station)
                & (temperature_classes.day_of_year == day),
                "temperature_class",
            ].values[0]

            result_SFH[day] = np.random.choice(
                np.array(
                    idp_df[
                        (idp_df.temperature_class == t_class)
                        & (idp_df.house == "SFH")
                    ].index.values
                ),
                houses_per_climate_zone.loc[station, "SFH"],
            )

            result_MFH[day] = np.random.choice(
                np.array(
                    idp_df[
                        (idp_df.temperature_class == t_class)
                        & (idp_df.house == "MFH")
                    ].index.values
                ),
                houses_per_climate_zone.loc[station, "MFH"],
            )

        result_SFH["zensus_population_id"] = (
            annual_demand[annual_demand.climate_zone == station]
            .loc[
                annual_demand[
                    annual_demand.climate_zone == station
                ].index.repeat(
                    annual_demand[
                        annual_demand.climate_zone == station
                    ].SFH.astype(int)
                )
            ]
            .index.values
        )

        result_SFH["building_id"] = (
            db.select_dataframe(
                """
        
            SELECT cell_id as zensus_population_id, building_id FROM 
            (
            SELECT cell_id, COUNT(*), building_id
            FROM demand.egon_household_electricity_profile_of_buildings
            GROUP BY (cell_id, building_id)
            ) a 
            WHERE a.count = 1
            """,
                index_col="zensus_population_id",
            )
            .loc[result_SFH["zensus_population_id"].unique(), "building_id"]
            .values
        )

        result_MFH["zensus_population_id"] = (
            annual_demand[annual_demand.climate_zone == station]
            .loc[
                annual_demand[
                    annual_demand.climate_zone == station
                ].index.repeat(
                    annual_demand[
                        annual_demand.climate_zone == station
                    ].MFH.astype(int)
                )
            ]
            .index.values
        )

        result_MFH["building_id"] = (
            db.select_dataframe(
                """
        
            SELECT cell_id as zensus_population_id, building_id FROM 
            (
            SELECT cell_id, COUNT(*), building_id
            FROM demand.egon_household_electricity_profile_of_buildings
            GROUP BY (cell_id, building_id)
            ) a 
            WHERE a.count > 1
            """,
                index_col="zensus_population_id",
            )
            .loc[result_MFH["zensus_population_id"].unique(), "building_id"]
            .values
        )

        df_sfh = pd.DataFrame(
            data={
                "selected_idp_profiles": result_SFH[
                    range(1, 366)
                ].values.tolist(),
                "zensus_population_id": (
                    annual_demand[annual_demand.climate_zone == station]
                    .loc[
                        annual_demand[
                            annual_demand.climate_zone == station
                        ].index.repeat(
                            annual_demand[
                                annual_demand.climate_zone == station
                            ].SFH.astype(int)
                        )
                    ]
                    .index.values
                ),
                "building_id": (
                    db.select_dataframe(
                        """
            
                SELECT cell_id as zensus_population_id, building_id FROM 
                (
                SELECT cell_id, COUNT(*), building_id
                FROM demand.egon_household_electricity_profile_of_buildings
                GROUP BY (cell_id, building_id)
                ) a 
                WHERE a.count = 1
                """,
                        index_col="zensus_population_id",
                    )
                    .loc[
                        result_SFH["zensus_population_id"].unique(),
                        "building_id",
                    ]
                    .values
                ),
            }
        )
        start_sfh = datetime.now()
        df_sfh.set_index(["zensus_population_id", "building_id"]).to_sql(
            "heat_timeseries_selected_profiles",
            schema="demand",
            con=db.engine(),
            if_exists="append",
            chunksize=5000,
            method="multi",
        )
        print(f"SFH insertation for zone {station}:")
        print(datetime.now() - start_sfh)

        df_mfh = pd.DataFrame(
            data={
                "selected_idp_profiles": result_MFH[
                    range(1, 366)
                ].values.tolist(),
                "zensus_population_id": (
                    annual_demand[annual_demand.climate_zone == station]
                    .loc[
                        annual_demand[
                            annual_demand.climate_zone == station
                        ].index.repeat(
                            annual_demand[
                                annual_demand.climate_zone == station
                            ].MFH.astype(int)
                        )
                    ]
                    .index.values
                ),
                "building_id": (
                    db.select_dataframe(
                        """
            
                SELECT cell_id as zensus_population_id, building_id FROM 
                (
                SELECT cell_id, COUNT(*), building_id
                FROM demand.egon_household_electricity_profile_of_buildings
                GROUP BY (cell_id, building_id)
                ) a 
                WHERE a.count > 1
                """,
                        index_col="zensus_population_id",
                    )
                    .loc[
                        result_MFH["zensus_population_id"].unique(),
                        "building_id",
                    ]
                    .values
                ),
            }
        )

        start_mfh = datetime.now()
        df_mfh.set_index(["zensus_population_id", "building_id"]).to_sql(
            "heat_timeseries_selected_profiles",
            schema="demand",
            con=db.engine(),
            if_exists="append",
            chunksize=5000,
            method="multi",
        )
        print(f"MFH insertation for zone {station}:")
        print(datetime.now() - start_mfh)

    print("Time for overall profile selection:")
    print(datetime.now() - start_profile_selector)


def h_value():
    """
    Description: Assignment of daily demand scaling factor to each day of all TRY Climate Zones

    Returns
    -------
    h : pandas.DataFrame
        Hourly factor values for each station corresponding to the temperature profile.
        Extracted from demandlib.

    """
    index = pd.date_range(datetime(2011, 1, 1, 0), periods=8760, freq="H")

    a = 3.0469695

    b = -37.1833141

    c = 5.6727847

    d = 0.1163157

    temp_profile = temperature_profile_extract()
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


def create_timeseries_for_building(building_id, scenario):
    """Generates final heat demand timeseries for a specific building

    Parameters
    ----------
    building_id : int
        Index of the selected building
    scenario : str
        Name of the selected scenario.

    Returns
    -------
    pandas.DataFrame
        Hourly heat demand timeseries in MW for the selected building

    """

    return db.select_dataframe(
        f"""
        SELECT building_demand * UNNEST(idp) as demand 
        FROM 
        (
        SELECT demand.demand / building.count * daily_demand.daily_demand_share as building_demand, daily_demand.day_of_year
        FROM 
        
        (SELECT demand FROM 
        demand.egon_peta_heat
        WHERE scenario = '{scenario}'
        AND sector = 'residential'
        AND zensus_population_id IN(
        SELECT zensus_population_id FROM 
        demand.heat_timeseries_selected_profiles
        WHERE building_id  = {building_id})) as demand,
        
        (SELECT COUNT(building_id)
        FROM demand.heat_timeseries_selected_profiles
        WHERE zensus_population_id IN(
        SELECT zensus_population_id FROM 
        demand.heat_timeseries_selected_profiles
        WHERE building_id  = {building_id})) as building,
        
        (SELECT daily_demand_share, day_of_year FROM 
        demand.egon_daily_heat_demand_per_climate_zone 
        WHERE climate_zone = (
            SELECT climate_zone FROM boundaries.egon_map_zensus_climate_zones
            WHERE zensus_population_id = 
            (SELECT zensus_population_id FROM demand.heat_timeseries_selected_profiles
             WHERE building_id = {building_id}))) as daily_demand) as daily_demand
        
        JOIN (SELECT b.idp, ordinality as day
        FROM demand.heat_timeseries_selected_profiles a,
        UNNEST (a.selected_idp_profiles) WITH ORDINALITY as selected_idp        
        JOIN demand.heat_idp_pool b
        ON selected_idp = b.index        
        WHERE a.building_id = {building_id}) as demand_profile
        ON demand_profile.day = daily_demand.day_of_year
        """
    )


def create_district_heating_profile(scenario, area_id):
    """Create heat demand profile for district heating grid including demands of
    households and service sector.

    Parameters
    ----------
    scenario : str
        Name of the selected scenario.
    area_id : int
        Index of the selected district heating grid

    Returns
    -------
    df : pandas,DataFrame
        Hourly heat demand timeseries in MW for the selected district heating grid

    """

    start_time = datetime.now()

    df = db.select_dataframe(
        f"""
        
        SELECT SUM(building_demand_per_hour) as demand_profile, hour_of_year
        FROM 
        
        (
        SELECT demand.demand  * 
        c.daily_demand_share * hourly_demand as building_demand_per_hour,
        ordinality + 24* (c.day_of_year-1) as hour_of_year,
        demand_profile.building_id,
        c.day_of_year,
        ordinality
        
        FROM 
        
        (SELECT zensus_population_id, demand FROM 
        demand.egon_peta_heat
        WHERE scenario = '{scenario}'
        AND sector = 'residential'
        AND zensus_population_id IN(
        SELECT zensus_population_id FROM 
        demand.egon_map_zensus_district_heating_areas
        WHERE scenario = '{scenario}'
        AND area_id = {area_id}
        )) as demand
        
        JOIN boundaries.egon_map_zensus_climate_zones b 
        ON demand.zensus_population_id = b.zensus_population_id
        
        JOIN demand.egon_daily_heat_demand_per_climate_zone c
        ON c.climate_zone = b.climate_zone
        
        JOIN (SELECT e.idp, ordinality as day, zensus_population_id, building_id
        FROM demand.heat_timeseries_selected_profiles d,
        UNNEST (d.selected_idp_profiles) WITH ORDINALITY as selected_idp        
        JOIN demand.heat_idp_pool e
        ON selected_idp = e.index
        WHERE zensus_population_id IN (
        SELECT zensus_population_id FROM 
        demand.egon_map_zensus_district_heating_areas
        WHERE scenario = '{scenario}'
        AND area_id = {area_id}
        ))  demand_profile
        ON (demand_profile.day = c.day_of_year AND 
            demand_profile.zensus_population_id = b.zensus_population_id)
        
        JOIN (SELECT COUNT(building_id), zensus_population_id
        FROM demand.heat_timeseries_selected_profiles
        WHERE zensus_population_id IN(
        SELECT zensus_population_id FROM 
        demand.heat_timeseries_selected_profiles
       WHERE zensus_population_id IN (
       SELECT zensus_population_id FROM 
       demand.egon_map_zensus_district_heating_areas
       WHERE scenario = '{scenario}'
       AND area_id = {area_id}
       ))
		GROUP BY zensus_population_id) building
        ON building.zensus_population_id = b.zensus_population_id,
        
        UNNEST(demand_profile.idp) WITH ORDINALITY as hourly_demand
        )   result
        
        
        GROUP BY hour_of_year

        """
    )

    print(
        f"Time to create time series for district heating grid {scenario} {area_id}:"
    )
    print(datetime.now() - start_time)

    return df


def create_district_heating_profile_python_like(scenario="eGon2035"):
    """Creates profiles for all district heating grids in one scenario.
    Similar to create_district_heating_profile but faster and needs more RAM.
    The results are directly written into the database.

    Parameters
    ----------
    scenario : str
        Name of the selected scenario.

    Returns
    -------
    None.

    """

    start_time = datetime.now()

    idp_df = db.select_dataframe(
        """
        SELECT index, idp FROM demand.heat_idp_pool
        """,
        index_col="index",
    )

    annual_demand = db.select_dataframe(
        f"""
        SELECT a.zensus_population_id, demand/c.count as per_building , area_id FROM 
        demand.egon_peta_heat a
        INNER JOIN (
            SELECT * FROM demand.egon_map_zensus_district_heating_areas
            WHERE scenario = '{scenario}'
        ) b ON a.zensus_population_id = b.zensus_population_id
        
        JOIN (SELECT COUNT(building_id), zensus_population_id
        FROM demand.heat_timeseries_selected_profiles
        WHERE zensus_population_id IN(
        SELECT zensus_population_id FROM 
        demand.heat_timeseries_selected_profiles
        WHERE zensus_population_id IN (
        SELECT zensus_population_id FROM 
        boundaries.egon_map_zensus_grid_districts
       )) 
        GROUP BY zensus_population_id)c
        ON a.zensus_population_id = c.zensus_population_id
        
        WHERE a.scenario = '{scenario}'
        AND a.sector = 'residential'
        
        """,
        index_col="zensus_population_id",
    )

    daily_demand_shares = db.select_dataframe(
        """
        SELECT climate_zone, day_of_year as day, daily_demand_share FROM 
        demand.egon_daily_heat_demand_per_climate_zone        
        """
    )

    selected_profiles = db.select_dataframe(
        f"""
        SELECT a.zensus_population_id, building_id, c.climate_zone, 
        selected_idp, ordinality as day, b.area_id
        FROM demand.heat_timeseries_selected_profiles a
        INNER JOIN boundaries.egon_map_zensus_climate_zones c
        ON a.zensus_population_id = c.zensus_population_id
        INNER JOIN (
            SELECT * FROM demand.egon_map_zensus_district_heating_areas
            WHERE scenario = '{scenario}'
        ) b ON a.zensus_population_id = b.zensus_population_id        ,
        
        UNNEST (selected_idp_profiles) WITH ORDINALITY as selected_idp 

        """
    )

    df = pd.merge(
        selected_profiles, daily_demand_shares, on=["day", "climate_zone"]
    )

    CTS_demand_dist, CTS_demand_grid, CTS_demand_zensus = CTS_demand_scale(
        aggregation_level="district"
    )

    # TODO: use session_scope!
    from sqlalchemy.orm import sessionmaker

    session = sessionmaker(bind=db.engine())()
    engine = db.engine()
    EgonTimeseriesDistrictHeating.__table__.drop(bind=engine, checkfirst=True)
    EgonTimeseriesDistrictHeating.__table__.create(
        bind=engine, checkfirst=True
    )
    print(
        f"Time to create overhead for time series for district heating scenario {scenario}"
    )
    print(datetime.now() - start_time)

    start_time = datetime.now()
    for area in annual_demand.area_id.unique():
        slice_df = pd.merge(
            df[df.area_id == area],
            idp_df,
            left_on="selected_idp",
            right_on="index",
        )

        for hour in range(24):
            slice_df[hour] = (
                slice_df.idp.str[hour]
                .mul(slice_df.daily_demand_share)
                .mul(
                    annual_demand.loc[
                        slice_df.zensus_population_id.values, "per_building"
                    ].values
                )
            )

        cts = CTS_demand_dist[
            (CTS_demand_dist.scenario == scenario)
            & (CTS_demand_dist.index == area)
        ].drop("scenario", axis="columns")

        hh = np.concatenate(
            slice_df.groupby("day").sum()[range(24)].values
        ).ravel()

        if not (slice_df[hour].empty or cts.empty):
            entry = EgonTimeseriesDistrictHeating(
                area_id=int(area),
                scenario=scenario,
                dist_aggregated_mw=(hh + cts.values[0]).tolist(),
            )
        elif not slice_df[hour].empty:
            entry = EgonTimeseriesDistrictHeating(
                area_id=int(area),
                scenario=scenario,
                dist_aggregated_mw=(hh).tolist(),
            )
        elif not cts.empty:
            entry = EgonTimeseriesDistrictHeating(
                area_id=int(area),
                scenario=scenario,
                dist_aggregated_mw=(cts).tolist(),
            )

        session.add(entry)
    session.commit()

    print(
        f"Time to create time series for district heating scenario {scenario}"
    )
    print(datetime.now() - start_time)


def create_individual_heat_per_mv_grid(scenario="eGon2035", mv_grid_id=1564):
    start_time = datetime.now()
    df = db.select_dataframe(
        f"""
        
        SELECT SUM(building_demand_per_hour) as demand_profile, hour_of_year
        FROM 
        
        (
        SELECT demand.demand  * 
        c.daily_demand_share * hourly_demand as building_demand_per_hour,
        ordinality + 24* (c.day_of_year-1) as hour_of_year,
        demand_profile.building_id,
        c.day_of_year,
        ordinality
        
        FROM 
        
        (SELECT zensus_population_id, demand FROM 
        demand.egon_peta_heat
        WHERE scenario = '{scenario}'
        AND sector = 'residential'
        AND zensus_population_id IN (
        SELECT zensus_population_id FROM 
        boundaries.egon_map_zensus_grid_districts
        WHERE bus_id = {mv_grid_id}
        )) as demand
        
        JOIN boundaries.egon_map_zensus_climate_zones b 
        ON demand.zensus_population_id = b.zensus_population_id
        
        JOIN demand.egon_daily_heat_demand_per_climate_zone c
        ON c.climate_zone = b.climate_zone
        
        JOIN (SELECT e.idp, ordinality as day, zensus_population_id, building_id
        FROM demand.heat_timeseries_selected_profiles d,
        UNNEST (d.selected_idp_profiles) WITH ORDINALITY as selected_idp        
        JOIN demand.heat_idp_pool e
        ON selected_idp = e.index
        WHERE zensus_population_id IN (
        SELECT zensus_population_id FROM 
        boundaries.egon_map_zensus_grid_districts
        WHERE bus_id = {mv_grid_id}
        ))  demand_profile
        ON (demand_profile.day = c.day_of_year AND 
            demand_profile.zensus_population_id = b.zensus_population_id)
        
        JOIN (SELECT COUNT(building_id), zensus_population_id
        FROM demand.heat_timeseries_selected_profiles
        WHERE zensus_population_id IN(
        SELECT zensus_population_id FROM 
        demand.heat_timeseries_selected_profiles
        WHERE zensus_population_id IN (
        SELECT zensus_population_id FROM 
        boundaries.egon_map_zensus_grid_districts
        WHERE bus_id = {mv_grid_id}
       ))
		GROUP BY zensus_population_id) building
        ON building.zensus_population_id = b.zensus_population_id,
        
        UNNEST(demand_profile.idp) WITH ORDINALITY as hourly_demand
        )   result
        
        
        GROUP BY hour_of_year

        """
    )

    print(f"Time to create time series for mv grid {scenario} {mv_grid_id}:")
    print(datetime.now() - start_time)

    return df


def create_individual_heating_profile_python_like(scenario="eGon2035"):

    start_time = datetime.now()

    idp_df = db.select_dataframe(
        f"""
        SELECT index, idp FROM demand.heat_idp_pool
        """,
        index_col="index",
    )

    annual_demand = db.select_dataframe(
        f"""
        SELECT a.zensus_population_id, demand/c.count as per_building, bus_id
        FROM demand.egon_peta_heat a

        
        JOIN (SELECT COUNT(building_id), zensus_population_id
        FROM demand.heat_timeseries_selected_profiles
        WHERE zensus_population_id IN(
        SELECT zensus_population_id FROM 
        demand.heat_timeseries_selected_profiles
        WHERE zensus_population_id IN (
        SELECT zensus_population_id FROM 
        boundaries.egon_map_zensus_grid_districts
       )) 
        GROUP BY zensus_population_id)c
        ON a.zensus_population_id = c.zensus_population_id
        
        JOIN boundaries.egon_map_zensus_grid_districts d
        ON a.zensus_population_id = d.zensus_population_id
        
        WHERE a.scenario = '{scenario}'
        AND a.sector = 'residential'
        AND a.zensus_population_id NOT IN (
            SELECT zensus_population_id FROM demand.egon_map_zensus_district_heating_areas
            WHERE scenario = '{scenario}'
        )
        
        """,
        index_col="zensus_population_id",
    )

    daily_demand_shares = db.select_dataframe(
        f"""
        SELECT climate_zone, day_of_year as day, daily_demand_share FROM 
        demand.egon_daily_heat_demand_per_climate_zone        
        """
    )

    CTS_demand_dist, CTS_demand_grid, CTS_demand_zensus = CTS_demand_scale(
        aggregation_level="district"
    )

    class EgonEtragoTimeseriesIndividualHeating(Base):
        __tablename__ = "egon_etrago_timeseries_individual_heating_new"
        __table_args__ = {"schema": "demand", "extend_existing": True}
        bus_id = Column(Integer, primary_key=True)
        scenario = Column(Text, primary_key=True)
        dist_aggregated_mw = Column(ARRAY(Float(53)))

    # TODO: use session_scope!
    from sqlalchemy.orm import sessionmaker

    session = sessionmaker(bind=db.engine())()
    engine = db.engine()
    EgonEtragoTimeseriesIndividualHeating.__table__.drop(
        bind=engine, checkfirst=True
    )
    EgonEtragoTimeseriesIndividualHeating.__table__.create(
        bind=engine, checkfirst=True
    )
    print(
        f"Time to create overhead for time series for district heating scenario {scenario}"
    )
    print(datetime.now() - start_time)

    start_time = datetime.now()
    for grid in annual_demand.bus_id.unique():

        selected_profiles = db.select_dataframe(
            f"""
            SELECT a.zensus_population_id, building_id, c.climate_zone, 
            selected_idp, ordinality as day
            FROM demand.heat_timeseries_selected_profiles a
            INNER JOIN boundaries.egon_map_zensus_climate_zones c
            ON a.zensus_population_id = c.zensus_population_id
            ,
            
            UNNEST (selected_idp_profiles) WITH ORDINALITY as selected_idp 
            
            WHERE a.zensus_population_id NOT IN (
                SELECT zensus_population_id FROM demand.egon_map_zensus_district_heating_areas
                WHERE scenario = '{scenario}'
            )
            AND a.zensus_population_id IN (
                SELECT zensus_population_id
                FROM boundaries.egon_map_zensus_grid_districts
                WHERE bus_id = '{grid}'
            )
    
            """
        )

        df = pd.merge(
            selected_profiles, daily_demand_shares, on=["day", "climate_zone"]
        )

        slice_df = pd.merge(
            df, idp_df, left_on="selected_idp", right_on="index"
        )

        for hour in range(24):
            slice_df[hour] = (
                slice_df.idp.str[hour]
                .mul(slice_df.daily_demand_share)
                .mul(
                    annual_demand.loc[
                        slice_df.zensus_population_id.values, "per_building"
                    ].values
                )
            )

        cts = CTS_demand_grid[
            (CTS_demand_grid.scenario == scenario)
            & (CTS_demand_grid.index == grid)
        ].drop("scenario", axis="columns")

        hh = np.concatenate(
            slice_df.groupby("day").sum()[range(24)].values
        ).ravel()

        if not (slice_df[hour].empty or cts.empty):
            entry = EgonEtragoTimeseriesIndividualHeating(
                bus_id=int(grid),
                scenario=scenario,
                dist_aggregated_mw=(hh + cts.values[0]).tolist(),
            )
        elif not slice_df[hour].empty:
            entry = EgonEtragoTimeseriesIndividualHeating(
                bus_id=int(grid),
                scenario=scenario,
                dist_aggregated_mw=(hh).tolist(),
            )
        elif not cts.empty:
            entry = EgonEtragoTimeseriesIndividualHeating(
                bus_id=int(grid),
                scenario=scenario,
                dist_aggregated_mw=(cts).tolist(),
            )

        session.add(entry)
    session.commit()

    print(
        f"Time to create time series for district heating scenario {scenario}"
    )
    print(datetime.now() - start_time)


def district_heating(method="python"):

    if method == "python":
        create_district_heating_profile_python_like("eGon2035")
        create_district_heating_profile_python_like("eGon100RE")

    else:
        engine = db.engine()
        EgonTimeseriesDistrictHeating.__table__.drop(
            bind=engine, checkfirst=True
        )
        EgonTimeseriesDistrictHeating.__table__.create(
            bind=engine, checkfirst=True
        )

        CTS_demand_dist, CTS_demand_grid, CTS_demand_zensus = CTS_demand_scale(
            aggregation_level="district"
        )

        ids = db.select_dataframe(
            """
            SELECT area_id, scenario
            FROM demand.egon_district_heating_areas
            """
        )

        df = pd.DataFrame(
            columns=["area_id", "scenario", "dist_aggregated_mw"]
        )

        for index, row in ids.iterrows():
            series = create_district_heating_profile(
                scenario=row.scenario, area_id=row.area_id
            )

            cts = (
                CTS_demand_dist[
                    (CTS_demand_dist.scenario == row.scenario)
                    & (CTS_demand_dist.index == row.area_id)
                ]
                .drop("scenario", axis="columns")
                .transpose()
            )

            if not cts.empty:
                data = (
                    cts[row.area_id] + series.demand_profile
                ).values.tolist()
            else:
                data = series.demand_profile.values.tolist()

            df = df.append(
                pd.Series(
                    data={
                        "area_id": row.area_id,
                        "scenario": row.scenario,
                        "dist_aggregated_mw": data,
                    },
                ),
                ignore_index=True,
            )

        df.to_sql(
            "egon_timeseries_district_heating",
            schema="demand",
            con=db.engine(),
            if_exists="append",
            index=False,
        )


def individual_heating_per_mv_grid(method="python"):

    if method == "python":
        create_individual_heating_profile_python_like("eGon2035")
        create_individual_heating_profile_python_like("eGon100RE")

    else:

        engine = db.engine()
        EgonEtragoTimeseriesIndividualHeating.__table__.drop(
            bind=engine, checkfirst=True
        )
        EgonEtragoTimeseriesIndividualHeating.__table__.create(
            bind=engine, checkfirst=True
        )

        CTS_demand_dist, CTS_demand_grid, CTS_demand_zensus = CTS_demand_scale(
            aggregation_level="district"
        )
        df = pd.DataFrame(columns=["bus_id", "scenario", "dist_aggregated_mw"])

        ids = db.select_dataframe(
            """
            SELECT bus_id
            FROM grid.egon_mv_grid_district
            """
        )

        for index, row in ids.iterrows():

            for scenario in ["eGon2035", "eGon100RE"]:
                series = create_individual_heat_per_mv_grid(
                    scenario, row.bus_id
                )
                cts = (
                    CTS_demand_grid[
                        (CTS_demand_grid.scenario == scenario)
                        & (CTS_demand_grid.index == row.bus_id)
                    ]
                    .drop("scenario", axis="columns")
                    .transpose()
                )
                if not cts.empty:
                    data = (
                        cts[row.bus_id] + series.demand_profile
                    ).values.tolist()
                else:
                    data = series.demand_profile.values.tolist()

                df = df.append(
                    pd.Series(
                        data={
                            "bus_id": row.bus_id,
                            "scenario": scenario,
                            "dist_aggregated_mw": data,
                        },
                    ),
                    ignore_index=True,
                )

        df.to_sql(
            "egon_etrago_timeseries_individual_heating",
            schema="demand",
            con=db.engine(),
            if_exists="append",
            index=False,
        )


def cts_demand_per_aggregation_level(aggregation_level, scenario):
    """

    Description: Create dataframe assigining the CTS demand curve to individual zensus cell
    based on their respective NUTS3 CTS curve

    Parameters
    ----------
    aggregation_level : str
        if further processing is to be done in zensus cell level 'other'
        else 'dsitrict'

    Returns
    -------
    CTS_per_district : pandas.DataFrame
        if aggregation ='district'
            NUTS3 CTS profiles assigned to individual
            zensu cells and aggregated per district heat area id
        else
            empty dataframe
    CTS_per_grid : pandas.DataFrame
        if aggregation ='district'
            NUTS3 CTS profiles assigned to individual
            zensu cells and aggregated per mv grid subst id
        else
            empty dataframe
    CTS_per_zensus : pandas.DataFrame
        if aggregation ='district'
            empty dataframe
        else
            NUTS3 CTS profiles assigned to individual
            zensu population id

    """

    # nuts_zensus = psycop_gdf_AF(
    #     "boundaries.egon_map_zensus_vg250", geom_column="zensus_geom"
    # )
    # nuts_zensus.drop("zensus_geom", axis=1, inplace=True)

    demand_nuts = db.select_dataframe(
        f"""
        SELECT demand, a.zensus_population_id, b.vg250_nuts3
        FROM demand.egon_peta_heat a 
        JOIN boundaries.egon_map_zensus_vg250 b 
        ON a.zensus_population_id = b.zensus_population_id
        
        WHERE a.sector = 'service'
        AND a.scenario = '{scenario}'
        ORDER BY a.zensus_population_id
        """
    )

    # demand_nuts = pd.merge(
    #     demand, nuts_zensus, how="left", on="zensus_population_id"
    # )

    # mv_grid = psycop_df_AF("boundaries.egon_map_zensus_grid_districts")

    if os.path.isfile("CTS_heat_demand_profile_nuts3.csv"):
        df_CTS_gas_2011 = pd.read_csv(
            "CTS_heat_demand_profile_nuts3.csv", index_col=0
        )
        df_CTS_gas_2011.columns.name = "ags_lk"
        df_CTS_gas_2011.index = pd.to_datetime(df_CTS_gas_2011.index)
        df_CTS_gas_2011 = df_CTS_gas_2011.asfreq("H")
    else:
        df_CTS_gas_2011 = temporal.disagg_temporal_gas_CTS(
            use_nuts3code=True, year=2011
        )
        df_CTS_gas_2011.to_csv("CTS_heat_demand_profile_nuts3.csv")

    ags_lk = pd.read_csv(
        os.path.join(
            os.getcwd(),
            "demandregio-disaggregator/disaggregator/disaggregator/data_in/regional",
            "t_nuts3_lk.csv",
        ),
        index_col=0,
    )
    ags_lk = ags_lk.drop(
        ags_lk.columns.difference(["natcode_nuts3", "ags_lk"]), axis=1
    )

    CTS_profile = df_CTS_gas_2011.transpose()
    CTS_profile.reset_index(inplace=True)
    CTS_profile.ags_lk = CTS_profile.ags_lk.astype(int)
    CTS_profile = pd.merge(CTS_profile, ags_lk, on="ags_lk", how="inner")
    CTS_profile.set_index("natcode_nuts3", inplace=True)
    CTS_profile.drop("ags_lk", axis=1, inplace=True)

    CTS_per_zensus = pd.merge(
        demand_nuts[["zensus_population_id", "vg250_nuts3"]],
        CTS_profile,
        left_on="vg250_nuts3",
        right_on=CTS_profile.index,
        how="left",
    )

    CTS_per_zensus = CTS_per_zensus.drop("vg250_nuts3", axis=1)

    if aggregation_level == "district":
        district_heating = db.select_dataframe(
            f"""
            SELECT area_id, zensus_population_id
            FROM demand.egon_map_zensus_district_heating_areas
            WHERE scenario = '{scenario}'
            """
        )

        # psycop_df_AF(
        #     "demand.egon_map_zensus_district_heating_areas"
        # )
        # district_heating = district_heating[
        #     district_heating.scenario == scenario
        # ]

        CTS_per_district = pd.merge(
            CTS_per_zensus,
            district_heating,
            on="zensus_population_id",
            how="inner",
        )
        CTS_per_district.set_index("area_id", inplace=True)
        CTS_per_district.drop("zensus_population_id", axis=1, inplace=True)

        CTS_per_district = CTS_per_district.groupby(lambda x: x, axis=0).sum()
        CTS_per_district = CTS_per_district.transpose()
        CTS_per_district = CTS_per_district.apply(lambda x: x / x.sum())
        CTS_per_district.columns.name = "area_id"
        CTS_per_district.reset_index(drop=True, inplace=True)

        # mv_grid = mv_grid.set_index("zensus_population_id")
        district_heating = district_heating.set_index("zensus_population_id")

        mv_grid_ind = db.select_dataframe(
            f"""
            SELECT bus_id, zensus_population_id
            FROM boundaries.egon_map_zensus_grid_districts
            WHERE zensus_population_id NOT IN 
            (SELECT zensus_population_id
             FROM demand.egon_map_zensus_district_heating_areas
             WHERE scenario = '{scenario}')
            """
        )

        # mv_grid.loc[
        #     mv_grid.index.difference(district_heating.index), :
        # ]
        # mv_grid_ind = mv_grid_ind.reset_index()

        CTS_per_grid = pd.merge(
            CTS_per_zensus,
            mv_grid_ind,
            on="zensus_population_id",
            how="inner",
        )
        CTS_per_grid.set_index("bus_id", inplace=True)
        CTS_per_grid.drop("zensus_population_id", axis=1, inplace=True)

        CTS_per_grid = CTS_per_grid.groupby(lambda x: x, axis=0).sum()
        CTS_per_grid = CTS_per_grid.transpose()
        CTS_per_grid = CTS_per_grid.apply(lambda x: x / x.sum())
        CTS_per_grid.columns.name = "bus_id"
        CTS_per_grid.reset_index(drop=True, inplace=True)

        CTS_per_zensus = pd.DataFrame()

    else:
        CTS_per_district = pd.DataFrame()
        CTS_per_grid = pd.DataFrame()
        CTS_per_zensus.set_index("zensus_population_id", inplace=True)

        CTS_per_zensus = CTS_per_zensus.groupby(lambda x: x, axis=0).sum()
        CTS_per_zensus = CTS_per_zensus.transpose()
        CTS_per_zensus = CTS_per_zensus.apply(lambda x: x / x.sum())
        CTS_per_zensus.columns.name = "zensus_population_id"
        CTS_per_zensus.reset_index(drop=True, inplace=True)

    return CTS_per_district, CTS_per_grid, CTS_per_zensus


def CTS_demand_scale(aggregation_level):
    """

    Description: caling the demand curves to the annual demand of the respective aggregation level


    Parameters
    ----------
    aggregation_level : str
        aggregation_level : str
        if further processing is to be done in zensus cell level 'other'
        else 'dsitrict'

    Returns
    -------
    CTS_per_district : pandas.DataFrame
        if aggregation ='district'
            Profiles scaled up to annual demand
        else
            0
    CTS_per_grid : pandas.DataFrame
        if aggregation ='district'
            Profiles scaled up to annual demandd
        else
            0
    CTS_per_zensus : pandas.DataFrame
        if aggregation ='district'
            0
        else
           Profiles scaled up to annual demand

    """
    scenarios = ["eGon2035", "eGon100RE"]

    all_heat_demand = psycop_df_AF("demand.egon_peta_heat")

    all_district_heating = psycop_df_AF(
        "demand.egon_map_zensus_district_heating_areas"
    )

    CTS_district = pd.DataFrame()
    CTS_grid = pd.DataFrame()
    CTS_zensus = pd.DataFrame()

    for scenario in scenarios:
        (
            CTS_per_district,
            CTS_per_grid,
            CTS_per_zensus,
        ) = cts_demand_per_aggregation_level(aggregation_level, scenario)
        CTS_per_district = CTS_per_district.transpose()
        CTS_per_grid = CTS_per_grid.transpose()
        CTS_per_zensus = CTS_per_zensus.transpose()

        demand = db.select_dataframe(
            f"""
                SELECT demand, zensus_population_id
                FROM demand.egon_peta_heat                
                WHERE sector = 'service'
                AND scenario = '{scenario}'
                ORDER BY zensus_population_id
                """
        )

        if aggregation_level == "district":

            district_heating = db.select_dataframe(
                f"""
                SELECT area_id, zensus_population_id
                FROM demand.egon_map_zensus_district_heating_areas
                WHERE scenario = '{scenario}'
                """
            )

            CTS_demands_district = pd.merge(
                demand,
                district_heating,
                on="zensus_population_id",
                how="inner",
            )
            CTS_demands_district.drop(
                "zensus_population_id", axis=1, inplace=True
            )
            CTS_demands_district = CTS_demands_district.groupby(
                "area_id"
            ).sum()

            CTS_per_district = pd.merge(
                CTS_per_district,
                CTS_demands_district[["demand"]],
                how="inner",
                right_on=CTS_per_district.index,
                left_on=CTS_demands_district.index,
            )

            CTS_per_district = CTS_per_district.rename(
                columns={"key_0": "area_id"}
            )
            CTS_per_district.set_index("area_id", inplace=True)

            CTS_per_district = CTS_per_district[
                CTS_per_district.columns[:-1]
            ].multiply(CTS_per_district.demand, axis=0)

            CTS_per_district.insert(0, "scenario", scenario)

            CTS_district = CTS_district.append(CTS_per_district)
            CTS_district = CTS_district.sort_index()

            mv_grid_ind = db.select_dataframe(
                f"""
                SELECT bus_id, zensus_population_id
                FROM boundaries.egon_map_zensus_grid_districts
                WHERE zensus_population_id NOT IN 
                (SELECT zensus_population_id
                 FROM demand.egon_map_zensus_district_heating_areas
                 WHERE scenario = '{scenario}')
                """
            )

            CTS_demands_grid = pd.merge(
                demand,
                mv_grid_ind[["bus_id", "zensus_population_id"]],
                on="zensus_population_id",
                how="inner",
            )

            CTS_demands_grid.drop("zensus_population_id", axis=1, inplace=True)
            CTS_demands_grid = CTS_demands_grid.groupby("bus_id").sum()

            CTS_per_grid = pd.merge(
                CTS_per_grid,
                CTS_demands_grid[["demand"]],
                how="inner",
                right_on=CTS_per_grid.index,
                left_on=CTS_demands_grid.index,
            )

            CTS_per_grid = CTS_per_grid.rename(columns={"key_0": "bus_id"})
            CTS_per_grid.set_index("bus_id", inplace=True)

            CTS_per_grid = CTS_per_grid[CTS_per_grid.columns[:-1]].multiply(
                CTS_per_grid.demand, axis=0
            )

            CTS_per_grid.insert(0, "scenario", scenario)

            CTS_grid = CTS_grid.append(CTS_per_grid)
            CTS_grid = CTS_grid.sort_index()

            CTS_per_zensus = 0

        else:
            CTS_per_district = 0
            CTS_per_grid = 0

            CTS_per_zensus = pd.merge(
                CTS_per_zensus,
                demand,
                how="inner",
                right_on=CTS_per_zensus.index,
                left_on=demand.zensus_population_id,
            )
            CTS_per_zensus = CTS_per_zensus.drop("key_0", axis=1)
            CTS_per_zensus.set_index("zensus_population_id", inplace=True)

            CTS_per_zensus = CTS_per_zensus[
                CTS_per_zensus.columns[:-1]
            ].multiply(CTS_per_zensus.demand, axis=0)
            CTS_per_zensus.insert(0, "scenario", scenario)

            CTS_per_zensus.reset_index(inplace=True)

            CTS_zensus = CTS_zensus.append(CTS_per_grid)
            CTS_zensus = CTS_zensus.set_index("bus_id")
            CTS_zensus = CTS_zensus.sort_index()

    return CTS_district, CTS_grid, CTS_zensus


# def store_national_profiles(
#     residential_demand_grid,
#     CTS_demand_grid,
#     residential_demand_dist,
#     CTS_demand_dist,
# ):

#     folder = Path(".") / "input-pypsa-eur-sec"
#     # Create the folder, if it does not exists already
#     if not os.path.exists(folder):
#         os.mkdir(folder)

#     for scenario in CTS_demand_grid.scenario.unique():
#         national_demand = pd.DataFrame(
#             columns=["residential rural", "services rural", "urban central"],
#             index=pd.date_range(
#                 datetime(2011, 1, 1, 0), periods=8760, freq="H"
#             ),
#         )

#         national_demand["residential rural"] = (
#             residential_demand_grid[
#                 residential_demand_grid.scenario == scenario
#             ]
#             .drop("scenario", axis="columns")
#             .sum()
#             .values
#         )
#         national_demand["services rural"] = (
#             CTS_demand_grid[CTS_demand_grid.scenario == scenario]
#             .sum(numeric_only=True)
#             .values
#         )
#         national_demand["urban central"] = (
#             residential_demand_dist[
#                 residential_demand_dist.scenario == scenario
#             ]
#             .drop("scenario", axis="columns")
#             .sum()
#             .values
#             + CTS_demand_dist[CTS_demand_dist.scenario == scenario]
#             .drop("scenario", axis="columns")
#             .sum()
#             .values
#         )

#         national_demand.to_csv(
#             folder / f"heat_demand_timeseries_DE_{scenario}.csv"
#         )


class HeatTimeSeries(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HeatTimeSeries",
            version="0.0.7.dev",
            dependencies=dependencies,
            tasks=(
                {
                    map_climate_zones_to_zensus,
                    daily_demand_shares_per_climate_zone,
                    idp_df_generator,
                },
                profile_selector,
                {district_heating, individual_heating_per_mv_grid},
            ),
        )
