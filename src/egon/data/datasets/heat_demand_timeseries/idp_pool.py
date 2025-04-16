from datetime import datetime
import os

from sqlalchemy import ARRAY, Column, Float, Integer, String
from sqlalchemy.ext.declarative import declarative_base

import numpy as np
import pandas as pd

from egon.data import db


from math import ceil

import egon


Base = declarative_base()


class EgonHeatTimeseries(Base):
    __tablename__ = "egon_heat_timeseries_selected_profiles"
    __table_args__ = {"schema": "demand"}
    zensus_population_id = Column(Integer, primary_key=True)
    building_id = Column(Integer, primary_key=True)
    selected_idp_profiles = Column(ARRAY(Integer))


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


def idp_pool_generator():
    """
    Create List of Dataframes for each temperature class for each household stock

    Returns
    -------
    list
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


def create():
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
        "egon_heat_idp_pool",
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


def select():
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
        FROM demand.egon_heat_idp_pool
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
            EgonHeatTimeseries.__table__.name,
            schema=EgonHeatTimeseries.__table__.schema,
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
            EgonHeatTimeseries.__table__.name,
            schema=EgonHeatTimeseries.__table__.schema,
            con=db.engine(),
            if_exists="append",
            chunksize=5000,
            method="multi",
        )
        print(f"MFH insertation for zone {station}:")
        print(datetime.now() - start_mfh)

    print("Time for overall profile selection:")
    print(datetime.now() - start_profile_selector)
