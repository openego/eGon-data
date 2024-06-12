from datetime import date, datetime
from pathlib import Path
import json
import os
import time

from sqlalchemy import ARRAY, Column, Float, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
import egon.data.datasets.era5 as era

try:
    from disaggregator import temporal
except ImportError as e:
    pass

from math import ceil

from egon.data.datasets import Dataset
from egon.data.datasets.heat_demand_timeseries.daily import (
    daily_demand_shares_per_climate_zone,
    map_climate_zones_to_zensus,
)
from egon.data.datasets.heat_demand_timeseries.idp_pool import create, select
from egon.data.datasets.heat_demand_timeseries.service_sector import (
    CTS_demand_scale,
)
from egon.data.metadata import (
    context,
    license_egon_data_odbl,
    meta_metadata,
    sources,
)

Base = declarative_base()


class EgonTimeseriesDistrictHeating(Base):
    __tablename__ = "egon_timeseries_district_heating"
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


class EgonIndividualHeatingPeakLoads(Base):
    __tablename__ = "egon_individual_heating_peak_loads"
    __table_args__ = {"schema": "demand"}
    building_id = Column(Integer, primary_key=True)
    scenario = Column(Text, primary_key=True)
    w_th = Column(Float(53))


class EgonEtragoHeatCts(Base):
    __tablename__ = "egon_etrago_heat_cts"
    __table_args__ = {"schema": "demand"}

    bus_id = Column(Integer, primary_key=True)
    scn_name = Column(String, primary_key=True)
    p_set = Column(ARRAY(Float))


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
        SELECT
            demand.demand
            / building.count
            * daily_demand.daily_demand_share as building_demand,
            daily_demand.day_of_year
        FROM

        (SELECT demand FROM
        demand.egon_peta_heat
        WHERE scenario = '{scenario}'
        AND sector = 'residential'
        AND zensus_population_id IN(
        SELECT zensus_population_id FROM
        demand.egon_heat_timeseries_selected_profiles
        WHERE building_id  = {building_id})) as demand,

        (SELECT COUNT(building_id)
        FROM demand.egon_heat_timeseries_selected_profiles
        WHERE zensus_population_id IN(
        SELECT zensus_population_id FROM
        demand.egon_heat_timeseries_selected_profiles
        WHERE building_id  = {building_id})) as building,

        (SELECT daily_demand_share, day_of_year FROM
        demand.egon_daily_heat_demand_per_climate_zone
        WHERE climate_zone = (
            SELECT climate_zone FROM boundaries.egon_map_zensus_climate_zones
            WHERE zensus_population_id =
            (
                SELECT zensus_population_id
                FROM demand.egon_heat_timeseries_selected_profiles
                WHERE building_id = {building_id}
            )
        )) as daily_demand) as daily_demand

        JOIN (SELECT b.idp, ordinality as day
        FROM demand.egon_heat_timeseries_selected_profiles a,
        UNNEST (a.selected_idp_profiles) WITH ORDINALITY as selected_idp
        JOIN demand.egon_heat_idp_pool b
        ON selected_idp = b.index
        WHERE a.building_id = {building_id}) as demand_profile
        ON demand_profile.day = daily_demand.day_of_year
        """
    )


def create_district_heating_profile(scenario, area_id):
    """Create a heat demand profile for a district heating grid.

    The created heat demand profile includes the demands of households
    and the service sector.

    Parameters
    ----------
    scenario : str
        The name of the selected scenario.
    area_id : int
        The index of the selected district heating grid.

    Returns
    -------
    pd.DataFrame
        An hourly heat demand timeseries in MW for the selected district
        heating grid.

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

        JOIN (
        SELECT e.idp, ordinality as day, zensus_population_id, building_id
        FROM demand.egon_heat_timeseries_selected_profiles d,
        UNNEST (d.selected_idp_profiles) WITH ORDINALITY as selected_idp
        JOIN demand.egon_heat_idp_pool e
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
        FROM demand.egon_heat_timeseries_selected_profiles
        WHERE zensus_population_id IN(
        SELECT zensus_population_id FROM
        demand.egon_heat_timeseries_selected_profiles
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
        f"Time to create time series for district heating grid {scenario}"
        f" {area_id}:\n{datetime.now() - start_time}"
    )

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
        SELECT index, idp FROM demand.egon_heat_idp_pool
        """,
        index_col="index",
    )

    district_heating_grids = db.select_dataframe(
        f"""
        SELECT area_id
        FROM demand.egon_district_heating_areas
        WHERE scenario = '{scenario}'
        """
    )

    annual_demand = db.select_dataframe(
        f"""
        SELECT
            a.zensus_population_id,
            demand / c.count as per_building,
            area_id,
            demand as demand_total
        FROM
        demand.egon_peta_heat a
        INNER JOIN (
            SELECT * FROM demand.egon_map_zensus_district_heating_areas
            WHERE scenario = '{scenario}'
        ) b ON a.zensus_population_id = b.zensus_population_id

        JOIN (SELECT COUNT(building_id), zensus_population_id
        FROM demand.egon_heat_timeseries_selected_profiles
        WHERE zensus_population_id IN(
        SELECT zensus_population_id FROM
        demand.egon_heat_timeseries_selected_profiles
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

    annual_demand = annual_demand[
        ~annual_demand.index.duplicated(keep="first")
    ]

    daily_demand_shares = db.select_dataframe(
        """
        SELECT climate_zone, day_of_year as day, daily_demand_share FROM
        demand.egon_daily_heat_demand_per_climate_zone
        """
    )

    CTS_demand_dist, CTS_demand_grid, CTS_demand_zensus = CTS_demand_scale(
        aggregation_level="district"
    )

    # TODO: use session_scope!
    from sqlalchemy.orm import sessionmaker

    session = sessionmaker(bind=db.engine())()

    print(datetime.now() - start_time)

    start_time = datetime.now()
    for area in district_heating_grids.area_id.unique():
        selected_profiles = db.select_dataframe(
            f"""
            SELECT a.zensus_population_id, building_id, c.climate_zone,
            selected_idp, ordinality as day, b.area_id
            FROM demand.egon_heat_timeseries_selected_profiles a
            INNER JOIN boundaries.egon_map_zensus_climate_zones c
            ON a.zensus_population_id = c.zensus_population_id
            INNER JOIN (
                SELECT * FROM demand.egon_map_zensus_district_heating_areas
                WHERE scenario = '{scenario}'
                AND area_id = '{area}'
            ) b ON a.zensus_population_id = b.zensus_population_id        ,

            UNNEST (selected_idp_profiles) WITH ORDINALITY as selected_idp

            """
        )

        if not selected_profiles.empty:
            df = pd.merge(
                selected_profiles,
                daily_demand_shares,
                on=["day", "climate_zone"],
            )

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
                            slice_df.zensus_population_id.values,
                            "per_building",
                        ].values
                    )
                )

            diff = (
                slice_df.groupby("day").sum()[range(24)].sum().sum()
                - annual_demand[
                    annual_demand.area_id == area
                ].demand_total.sum()
            ) / (
                annual_demand[annual_demand.area_id == area].demand_total.sum()
            )

            assert (
                abs(diff) < 0.03
            ), f"""Deviation of residential heat demand time
            series for district heating grid {str(area)} is {diff}"""

            hh = np.concatenate(
                slice_df.groupby("day").sum()[range(24)].values
            ).ravel()

        cts = CTS_demand_dist[
            (CTS_demand_dist.scenario == scenario)
            & (CTS_demand_dist.index == area)
        ].drop("scenario", axis="columns")

        if (not selected_profiles.empty) and not cts.empty:
            entry = EgonTimeseriesDistrictHeating(
                area_id=int(area),
                scenario=scenario,
                dist_aggregated_mw=(hh + cts.values[0]).tolist(),
            )
        elif (not selected_profiles.empty) and cts.empty:
            entry = EgonTimeseriesDistrictHeating(
                area_id=int(area),
                scenario=scenario,
                dist_aggregated_mw=(hh).tolist(),
            )
        elif not cts.empty:
            entry = EgonTimeseriesDistrictHeating(
                area_id=int(area),
                scenario=scenario,
                dist_aggregated_mw=(cts.values[0]).tolist(),
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

        JOIN (
        SELECT
            e.idp, ordinality as day, zensus_population_id, building_id
        FROM demand.egon_heat_timeseries_selected_profiles d,
        UNNEST (d.selected_idp_profiles) WITH ORDINALITY as selected_idp
        JOIN demand.egon_heat_idp_pool e
        ON selected_idp = e.index
        WHERE zensus_population_id IN (
        SELECT zensus_population_id FROM
        boundaries.egon_map_zensus_grid_districts
        WHERE bus_id = {mv_grid_id}
        ))  demand_profile
        ON (demand_profile.day = c.day_of_year AND
            demand_profile.zensus_population_id = b.zensus_population_id)

        JOIN (SELECT COUNT(building_id), zensus_population_id
        FROM demand.egon_heat_timeseries_selected_profiles
        WHERE zensus_population_id IN(
        SELECT zensus_population_id FROM
        demand.egon_heat_timeseries_selected_profiles
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


def calulate_peak_load(df, scenario):
    # peat load in W_th
    data = (
        df.groupby("building_id")
        .max()[range(24)]
        .max(axis=1)
        .mul(1000000)
        .astype(int)
        .reset_index()
    )

    data["scenario"] = scenario

    data.rename({0: "w_th"}, axis="columns", inplace=True)

    data.to_sql(
        EgonIndividualHeatingPeakLoads.__table__.name,
        schema=EgonIndividualHeatingPeakLoads.__table__.schema,
        con=db.engine(),
        if_exists="append",
        index=False,
    )


def create_individual_heating_peak_loads(scenario="eGon2035"):
    engine = db.engine()

    EgonIndividualHeatingPeakLoads.__table__.drop(bind=engine, checkfirst=True)

    EgonIndividualHeatingPeakLoads.__table__.create(
        bind=engine, checkfirst=True
    )

    start_time = datetime.now()

    idp_df = db.select_dataframe(
        """
        SELECT index, idp FROM demand.egon_heat_idp_pool
        """,
        index_col="index",
    )

    annual_demand = db.select_dataframe(
        f"""
        SELECT a.zensus_population_id, demand/c.count as per_building, bus_id
        FROM demand.egon_peta_heat a


        JOIN (SELECT COUNT(building_id), zensus_population_id
        FROM demand.egon_heat_timeseries_selected_profiles
        WHERE zensus_population_id IN(
        SELECT zensus_population_id FROM
        demand.egon_heat_timeseries_selected_profiles
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
            SELECT zensus_population_id
            FROM demand.egon_map_zensus_district_heating_areas
            WHERE scenario = '{scenario}'
        )

        """,
        index_col="zensus_population_id",
    )

    daily_demand_shares = db.select_dataframe(
        """
        SELECT climate_zone, day_of_year as day, daily_demand_share FROM
        demand.egon_daily_heat_demand_per_climate_zone
        """
    )

    start_time = datetime.now()
    for grid in annual_demand.bus_id.unique():
        selected_profiles = db.select_dataframe(
            f"""
            SELECT a.zensus_population_id, building_id, c.climate_zone,
            selected_idp, ordinality as day
            FROM demand.egon_heat_timeseries_selected_profiles a
            INNER JOIN boundaries.egon_map_zensus_climate_zones c
            ON a.zensus_population_id = c.zensus_population_id
            ,

            UNNEST (selected_idp_profiles) WITH ORDINALITY as selected_idp

            WHERE a.zensus_population_id NOT IN (
                SELECT zensus_population_id
                FROM demand.egon_map_zensus_district_heating_areas
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

        calulate_peak_load(slice_df, scenario)

    print(f"Time to create peak loads per building for {scenario}")
    print(datetime.now() - start_time)


def create_individual_heating_profile_python_like(scenario="eGon2035"):
    start_time = datetime.now()

    idp_df = db.select_dataframe(
        f"""
        SELECT index, idp FROM demand.egon_heat_idp_pool
        """,
        index_col="index",
    )

    annual_demand = db.select_dataframe(
        f"""
        SELECT
            a.zensus_population_id,
            demand / c.count as per_building,
            demand as demand_total,
            bus_id
        FROM demand.egon_peta_heat a


        JOIN (SELECT COUNT(building_id), zensus_population_id
        FROM demand.egon_heat_timeseries_selected_profiles
        WHERE zensus_population_id IN(
        SELECT zensus_population_id FROM
        demand.egon_heat_timeseries_selected_profiles
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
            SELECT zensus_population_id
            FROM demand.egon_map_zensus_district_heating_areas
            WHERE scenario = '{scenario}'
        )

        """,
        index_col="zensus_population_id",
    )

    daily_demand_shares = db.select_dataframe(
        """
        SELECT climate_zone, day_of_year as day, daily_demand_share FROM
        demand.egon_daily_heat_demand_per_climate_zone
        """
    )

    CTS_demand_dist, CTS_demand_grid, CTS_demand_zensus = CTS_demand_scale(
        aggregation_level="district"
    )

    # TODO: use session_scope!
    from sqlalchemy.orm import sessionmaker

    session = sessionmaker(bind=db.engine())()

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
            FROM demand.egon_heat_timeseries_selected_profiles a
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

        diff = (
            slice_df.groupby("day").sum()[range(24)].sum().sum()
            - annual_demand[annual_demand.bus_id == grid].demand_total.sum()
        ) / (annual_demand[annual_demand.bus_id == grid].demand_total.sum())

        assert abs(diff) < 0.03, (
            "Deviation of residential heat demand time series for mv"
            f" grid {grid} is {diff}"
        )

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
    engine = db.engine()
    EgonTimeseriesDistrictHeating.__table__.drop(bind=engine, checkfirst=True)
    EgonTimeseriesDistrictHeating.__table__.create(
        bind=engine, checkfirst=True
    )

    if method == "python":
        create_district_heating_profile_python_like("eGon2035")
        create_district_heating_profile_python_like("eGon100RE")

    else:
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


def individual_heating_per_mv_grid_tables(method="python"):
    engine = db.engine()
    EgonEtragoTimeseriesIndividualHeating.__table__.drop(
        bind=engine, checkfirst=True
    )
    EgonEtragoTimeseriesIndividualHeating.__table__.create(
        bind=engine, checkfirst=True
    )


def individual_heating_per_mv_grid_2035(method="python"):
    create_individual_heating_profile_python_like("eGon2035")


def individual_heating_per_mv_grid_100(method="python"):
    create_individual_heating_profile_python_like("eGon100RE")


def individual_heating_per_mv_grid(method="python"):
    if method == "python":
        engine = db.engine()
        EgonEtragoTimeseriesIndividualHeating.__table__.drop(
            bind=engine, checkfirst=True
        )
        EgonEtragoTimeseriesIndividualHeating.__table__.create(
            bind=engine, checkfirst=True
        )

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


def store_national_profiles():
    scenario = "eGon100RE"

    df = db.select_dataframe(
        f"""

        SELECT SUM(building_demand_per_hour) as "residential rural"
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
       ) as demand

        JOIN boundaries.egon_map_zensus_climate_zones b
        ON demand.zensus_population_id = b.zensus_population_id

        JOIN demand.egon_daily_heat_demand_per_climate_zone c
        ON c.climate_zone = b.climate_zone

        JOIN (
        SELECT e.idp, ordinality as day, zensus_population_id, building_id
        FROM demand.egon_heat_timeseries_selected_profiles d,
        UNNEST (d.selected_idp_profiles) WITH ORDINALITY as selected_idp
        JOIN demand.egon_heat_idp_pool e
        ON selected_idp = e.index
        )  demand_profile
        ON (demand_profile.day = c.day_of_year AND
            demand_profile.zensus_population_id = b.zensus_population_id)

        JOIN (SELECT COUNT(building_id), zensus_population_id
        FROM demand.egon_heat_timeseries_selected_profiles
        WHERE zensus_population_id IN(
        SELECT zensus_population_id FROM
        demand.egon_heat_timeseries_selected_profiles
        )
        GROUP BY zensus_population_id) building
        ON building.zensus_population_id = b.zensus_population_id,

        UNNEST(demand_profile.idp) WITH ORDINALITY as hourly_demand
        )   result


        GROUP BY hour_of_year

        """
    )

    CTS_demand_dist, CTS_demand_grid, CTS_demand_zensus = CTS_demand_scale(
        aggregation_level="district"
    )

    df["service rural"] = (
        CTS_demand_dist.loc[CTS_demand_dist.scenario == scenario]
        .drop("scenario", axis=1)
        .sum()
    )

    df["urban central"] = db.select_dataframe(
        f"""
        SELECT sum(demand) as "urban central"

        FROM demand.egon_timeseries_district_heating,
        UNNEST (dist_aggregated_mw) WITH ORDINALITY as demand

        WHERE scenario = '{scenario}'

        GROUP BY ordinality

        """
    )

    folder = Path(".") / "input-pypsa-eur-sec"
    # Create the folder, if it does not exists already
    if not os.path.exists(folder):
        os.mkdir(folder)

    df.to_csv(folder / f"heat_demand_timeseries_DE_{scenario}.csv")


def export_etrago_cts_heat_profiles():
    """Export heat cts load profiles at mv substation level
    to etrago-table in the database

    Returns
    -------
    None.

    """

    # Calculate cts heat profiles at substation
    _, CTS_grid, _ = CTS_demand_scale("district")

    # Change format
    data = CTS_grid.drop(columns="scenario")
    df_etrago_cts_heat_profiles = pd.DataFrame(
        index=data.index, columns=["scn_name", "p_set"]
    )
    df_etrago_cts_heat_profiles.p_set = data.values.tolist()
    df_etrago_cts_heat_profiles.scn_name = CTS_grid["scenario"]
    df_etrago_cts_heat_profiles.reset_index(inplace=True)

    # Drop and recreate Table if exists
    EgonEtragoHeatCts.__table__.drop(bind=db.engine(), checkfirst=True)
    EgonEtragoHeatCts.__table__.create(bind=db.engine(), checkfirst=True)

    # Write heat ts into db
    with db.session_scope() as session:
        session.bulk_insert_mappings(
            EgonEtragoHeatCts,
            df_etrago_cts_heat_profiles.to_dict(orient="records"),
        )


def metadata():
    fields = [
        {
            "description": "Index of corresponding district heating area",
            "name": "area_id",
            "type": "integer",
            "unit": "none",
        },
        {
            "description": "Name of scenario",
            "name": "scenario",
            "type": "str",
            "unit": "none",
        },
        {
            "description": "Heat demand time series",
            "name": "dist_aggregated_mw",
            "type": "array of floats",
            "unit": "MW",
        },
    ]

    meta_district = {
        "name": "demand.egon_timeseries_district_heating",
        "title": "eGon heat demand time series for district heating grids",
        "id": "WILL_BE_SET_AT_PUBLICATION",
        "description": "Heat demand time series for district heating grids",
        "language": ["EN"],
        "publicationDate": date.today().isoformat(),
        "context": context(),
        "spatial": {
            "location": None,
            "extent": "Germany",
            "resolution": None,
        },
        "sources": [
            sources()["era5"],
            sources()["vg250"],
            sources()["egon-data"],
            sources()["egon-data_bundle"],
            sources()["peta"],
        ],
        "licenses": [license_egon_data_odbl()],
        "contributors": [
            {
                "title": "Clara Büttner",
                "email": "http://github.com/ClaraBuettner",
                "date": time.strftime("%Y-%m-%d"),
                "object": None,
                "comment": "Imported data",
            },
        ],
        "resources": [
            {
                "profile": "tabular-data-resource",
                "name": "demand.egon_timeseries_district_heating",
                "path": None,
                "format": "PostgreSQL",
                "encoding": "UTF-8",
                "schema": {
                    "fields": fields,
                    "primaryKey": ["index"],
                    "foreignKeys": [],
                },
                "dialect": {"delimiter": None, "decimalSeparator": "."},
            }
        ],
        "metaMetadata": meta_metadata(),
    }

    # Add metadata as a comment to the table
    db.submit_comment(
        "'" + json.dumps(meta_district) + "'",
        EgonTimeseriesDistrictHeating.__table__.schema,
        EgonTimeseriesDistrictHeating.__table__.name,
    )



class HeatTimeSeries(Dataset):
    """
    Chooses heat demand profiles for each residential and CTS building

    This dataset creates heat demand profiles in an hourly resoultion.
    Time series for CTS buildings are created using the SLP-gas method implemented
    in the demandregio disagregator with the function :py:func:`export_etrago_cts_heat_profiles`
    and stored in the database.
    Time series for residential buildings are created based on a variety of synthetical created
    individual demand profiles that are part of :py:class:`DataBundle <egon.data.datasets.data_bundle.DataBundle>`.
    This method is desribed within the functions and in this publication:
        C. Büttner, J. Amme, J. Endres, A. Malla, B. Schachler, I. Cußmann,
        Open modeling of electricity and heat demand curves for all
        residential buildings in Germany, Energy Informatics 5 (1) (2022) 21.
        doi:10.1186/s42162-022-00201-y.


    *Dependencies*
      * :py:class:`DataBundle <egon.data.datasets.data_bundle.DataBundle>`
      * :py:class:`DemandRegio <egon.data.datasets.demandregio.DemandRegio>`
      * :py:class:`HeatDemandImport <egon.data.datasets.heat_demand.HeatDemandImport>`
      * :py:class:`DistrictHeatingAreas <egon.data.datasets.district_heating_areas.DistrictHeatingAreas>`
      * :py:class:`Vg250 <egon.data.datasets.vg250.Vg250>`
      * :py:class:`ZensusMvGridDistricts <egon.data.datasets.zensus_mv_grid_districts.ZensusMvGridDistricts>`
      * :py:func:`hh_demand_buildings_setup <egon.data.datasets.electricity_demand_timeseries.hh_buildings.map_houseprofiles_to_buildings>`
      * :py:class:`WeatherData <egon.data.datasets.era5.WeatherData>`


    *Resulting tables*
      * :py:class:`demand.egon_timeseries_district_heating <egon.data.datasets.heat_demand_timeseries.EgonTimeseriesDistrictHeating>` is created and filled
      * :py:class:`demand.egon_etrago_heat_cts <egon.data.datasets.heat_demand_timeseries.EgonEtragoHeatCts>` is created and filled
      * :py:class:`demand.egon_heat_timeseries_selected_profiles <egon.data.datasets.heat_demand_timeseries.idp_pool.EgonHeatTimeseries>` is created and filled
      * :py:class:`demand.egon_daily_heat_demand_per_climate_zone <egon.data.datasets.heat_demand_timeseries.daily.EgonDailyHeatDemandPerClimateZone>`
        is created and filled
      * :py:class:`boundaries.egon_map_zensus_climate_zones <egon.data.datasets.heat_demand_timeseries.daily.EgonMapZensusClimateZones>` is created and filled

    """

    #:
    name: str = "HeatTimeSeries"
    #:
    version: str = "0.0.8"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(
                {
                    export_etrago_cts_heat_profiles,
                    map_climate_zones_to_zensus,
                    daily_demand_shares_per_climate_zone,
                    create,
                },
                select,
                district_heating,
                metadata,
                # store_national_profiles,
            ),
        )
