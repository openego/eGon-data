from datetime import datetime
import os

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
import egon

from egon.data.datasets.heat_demand_timeseries.idp_pool import create, select
from egon.data.datasets.heat_demand_timeseries.service_sector import (
    CTS_demand_scale,
)
from egon.data.datasets.heat_demand_timeseries.daily import (
    map_climate_zones_to_zensus,
    daily_demand_shares_per_climate_zone,
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

    district_heating_grids = db.select_dataframe(
        f"""
        SELECT area_id 
        FROM demand.egon_district_heating_areas
        WHERE scenario = '{scenario}'
        """
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
    for area in district_heating_grids.area_id.unique():

        if area in df.area_id.values:
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

            hh = np.concatenate(
                slice_df.groupby("day").sum()[range(24)].values
            ).ravel()

        cts = CTS_demand_dist[
            (CTS_demand_dist.scenario == scenario)
            & (CTS_demand_dist.index == area)
        ].drop("scenario", axis="columns")

        if (area in df.area_id.values) and not cts.empty:
            entry = EgonTimeseriesDistrictHeating(
                area_id=int(area),
                scenario=scenario,
                dist_aggregated_mw=(hh + cts.values[0]).tolist(),
            )
        elif (area in df.area_id.values) and cts.empty:
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
                    create,
                },
                select,
                {district_heating, individual_heating_per_mv_grid},
            ),
        )
