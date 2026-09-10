"""
The central module creating heat demand time series for the eTraGo tool
"""

import numpy as np
import pandas as pd

from egon.data import config, db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.scenario_parameters import get_sector_parameters
from egon.data.db import next_etrago_id


def hts_to_etrago(scenario):
    sources = HtsEtragoTable.sources
    targets = HtsEtragoTable.targets
    carriers = ["central_heat", "rural_heat", "rural_gas_boiler"]

    if "status" in scenario:
        carriers = ["central_heat", "rural_heat"]

    for carrier in carriers:
        if carrier == "central_heat":
            # Map heat buses to district heating id and area_id
            # interlinking bus_id and area_id
            bus_area = db.select_dataframe(
                f"""
                 SELECT bus_id, area_id, id FROM
                 {sources.tables["heat_buses"]}
                 JOIN {sources.tables["district_heating_areas"]}
                 ON ST_Transform(ST_Centroid(geom_polygon), 4326) = geom
                 WHERE carrier = '{carrier}'
                 AND scenario='{scenario}'
                 AND scn_name = '{scenario}'
                 """,
                index_col="id",
            )

            # district heating time series time series
            disct_time_series = db.select_dataframe(f"""
                                SELECT * FROM 
                                {sources.tables["district_heating_timeseries"]}
                                WHERE scenario ='{scenario}'                                
                                """)
            # bus_id connected to corresponding time series
            bus_ts = pd.merge(
                bus_area, disct_time_series, on="area_id", how="inner"
            )

        elif carrier == "rural_heat":
            # interlinking heat_bus_id and mv_grid bus_id
            bus_sub = db.select_dataframe(f"""
                 SELECT 
                     {sources.tables["heat_buses"]}.bus_id as heat_bus_id,
                     {sources.tables["egon_mv_grid_district"]}.bus_id as bus_id
                FROM {sources.tables["heat_buses"]}
                JOIN {sources.tables["egon_mv_grid_district"]}
                ON ST_Transform(
                        ST_Centroid({sources.tables["egon_mv_grid_district"]}.geom),
                        4326
                    ) = {sources.tables["heat_buses"]}.geom
                 WHERE carrier = '{carrier}'
                 AND scn_name = '{scenario}'
                 """)
            ##**scenario name still needs to be adjusted in bus_sub**

            # individual heating time series
            ind_time_series = db.select_dataframe(f"""
                SELECT scenario, bus_id, dist_aggregated_mw
                FROM {sources.tables["individual_heating_timeseries"]}
                WHERE scenario ='{scenario}'
                AND carrier = 'heat_pump'
                """)

            # bus_id connected to corresponding time series
            bus_ts = pd.merge(
                bus_sub, ind_time_series, on="bus_id", how="inner"
            )

            # Connect  heat loads to heat buses
            bus_ts.loc[:, "bus_id"] = bus_ts.loc[:, "heat_bus_id"]

        else:
            efficiency_gas_boiler = get_sector_parameters("heat", scenario)[
                "efficiency"
            ]["rural_gas_boiler"]

            # Select rural heat demand coverd by individual gas boilers
            ind_time_series = db.select_dataframe(f"""
                SELECT * 
                FROM {sources.tables["individual_heating_timeseries"]}
                WHERE scenario ='{scenario}'
                AND carrier = 'CH4'
                """)

            # Select geoetry of medium voltage grid districts
            mvgd_geom = db.select_geodataframe(f"""
                SELECT bus_id, ST_CENTROID(geom) as geom
                FROM {sources.tables["egon_mv_grid_district"]}
                """)

            # Select geometry of gas (CH4) voronoi
            gas_voronoi = db.select_geodataframe(f"""
                SELECT bus_id, geom
                FROM {sources.tables["ch4_voronoi"]}
                WHERE scn_name = '{scenario}'
                AND carrier = 'CH4'
                """)

            # Map centroid of mvgd to gas voronoi
            join = mvgd_geom.sjoin(gas_voronoi, lsuffix="AC", rsuffix="gas")[
                ["bus_id_AC", "bus_id_gas"]
            ].set_index("bus_id_AC")

            # Assign gas bus to each rural heat demand coverd by gas boiler
            ind_time_series["gas_bus"] = join.loc[
                ind_time_series.bus_id
            ].values

            # Initialize dataframe to store final heat demand per gas node
            gas_ts = pd.DataFrame(
                index=ind_time_series["gas_bus"].unique(), columns=range(8760)
            )

            # Group heat demand per hour in the year
            for i in range(8760):
                gas_ts[i] = (
                    ind_time_series.set_index("gas_bus")
                    .dist_aggregated_mw.str[i]
                    .groupby("gas_bus")
                    .sum()
                    .div(efficiency_gas_boiler)
                )

            # Prepare resulting DataFrame
            bus_ts = pd.DataFrame(columns=["dist_aggregated_mw", "bus_id"])

            # Insert values to dataframe
            bus_ts.dist_aggregated_mw = gas_ts.values.tolist()
            bus_ts.bus_id = gas_ts.index

        # Delete existing data from database
        db.execute_sql(f"""
            DELETE FROM {targets.tables["loads"]}
            WHERE scn_name = '{scenario}'
            AND carrier = '{carrier}'
            AND bus IN (
                SELECT bus_id FROM {sources.tables["heat_buses"]}
                WHERE country = 'DE'
                AND scn_name = '{scenario}'
                )
            """)

        db.execute_sql(f"""
            DELETE FROM {targets.tables["load_timeseries"]}
            WHERE scn_name = '{scenario}'
            AND load_id NOT IN (
            SELECT load_id FROM {targets.tables["loads"]}
            WHERE scn_name = '{scenario}')
            """)

        bus_ts["load_id"] = db.next_etrago_id("load", len(bus_ts))

        etrago_load = pd.DataFrame(index=range(len(bus_ts)))
        etrago_load["scn_name"] = scenario
        etrago_load["load_id"] = bus_ts.load_id
        etrago_load["bus"] = bus_ts.bus_id
        etrago_load["carrier"] = carrier
        etrago_load["sign"] = -1

        etrago_load.to_sql(
            targets.get_table_name("loads"),
            schema=targets.get_table_schema("loads"),
            con=db.engine(),
            if_exists="append",
            index=False,
        )

        etrago_load_timeseries = pd.DataFrame(index=range(len(bus_ts)))
        etrago_load_timeseries["scn_name"] = scenario
        etrago_load_timeseries["load_id"] = bus_ts.load_id
        etrago_load_timeseries["temp_id"] = 1
        etrago_load_timeseries["p_set"] = bus_ts.loc[:, "dist_aggregated_mw"]

        etrago_load_timeseries.to_sql(
            targets.get_table_name("load_timeseries"),
            schema=targets.get_table_schema("load_timeseries"),
            con=db.engine(),
            if_exists="append",
            index=False,
        )


def demand():
    """Insert demand timeseries for heat into eTraGo tables

    Returns
    -------
    None.

    """
    for scenario in config.settings()["egon-data"]["--scenarios"]:
        hts_to_etrago(scenario)


class HtsEtragoTable(Dataset):
    """
    Collect heat demand time series for the eTraGo tool

    This dataset collects data for individual and district heating demands
    and writes that into the tables that can be read by the eTraGo tool.

    *Dependencies*
      * :py:class:`DistrictHeatingAreas <egon.data.datasets.district_heating_areas.DistrictHeatingAreas>`
      * :py:class:`HeatEtrago <egon.data.datasets.heat_etrago.HeatEtrago>`
      * :py:class:`MvGridDistricts <egon.data.datasets.mv_grid_districts.mv_grid_districts_setup>`
      * :py:class:`HeatPumpsCascade <egon.data.datasets.heat_supply.individual_heating.HeatPumpsCascade>`
      * :py:class:`HeatTimeSeries <egon.data.datasets.heat_demand_timeseries.HeatTimeSeries>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_load <egon.data.datasets.etrago_setup.EgonPfHvLoad>` is extended
      * :py:class:`grid.egon_etrago_load_timeseries <egon.data.datasets.etrago_setup.EgonPfHvLoadTimeseries>` is extended

    """

    #:
    name: str = "HtsEtragoTable"
    #:
    version: str = "0.0.9"

    sources = DatasetSources(
        tables={
            "heat_buses": "grid.egon_etrago_bus",
            "district_heating_areas": "demand.egon_district_heating_areas",
            "egon_mv_grid_district": "grid.egon_mv_grid_district",
            "ch4_voronoi": "grid.egon_gas_voronoi",
            "district_heating_timeseries": "demand.egon_timeseries_district_heating",
            "individual_heating_timeseries": "demand.egon_etrago_timeseries_individual_heating",
        },
    )

    targets = DatasetTargets(
        tables={
            "loads": "grid.egon_etrago_load",
            "load_timeseries": "grid.egon_etrago_load_timeseries",
        },
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(demand,),
        )
