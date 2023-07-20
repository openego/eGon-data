"""
The central module creating heat demand time series for the eTraGo tool
"""
from egon.data import config, db
from egon.data.db import next_etrago_id
from egon.data.datasets import Dataset
from egon.data.datasets.scenario_parameters import get_sector_parameters

import pandas as pd
import numpy as np


def hts_to_etrago():
    sources = config.datasets()["etrago_heat"]["sources"]
    targets = config.datasets()["etrago_heat"]["targets"]
    scenario = "eGon2035"
    carriers = ["central_heat", "rural_heat", "rural_gas_boiler"]

    for carrier in carriers:
        if carrier == "central_heat":
            # Map heat buses to district heating id and area_id
            # interlinking bus_id and area_id
            bus_area = db.select_dataframe(
                f"""
                 SELECT bus_id, area_id, id FROM
                 {targets['heat_buses']['schema']}.
                 {targets['heat_buses']['table']}
                 JOIN {sources['district_heating_areas']['schema']}.
                     {sources['district_heating_areas']['table']}
                 ON ST_Transform(ST_Centroid(geom_polygon), 4326) = geom
                 WHERE carrier = '{carrier}'
                 AND scenario='{scenario}'
                 AND scn_name = '{scenario}'
                 """,
                index_col="id",
            )

            # district heating time series time series
            disct_time_series = db.select_dataframe(
                f"""
                                SELECT * FROM 
                                demand.egon_timeseries_district_heating
                                WHERE scenario ='{scenario}'                                
                                """
            )
            # bus_id connected to corresponding time series
            bus_ts = pd.merge(
                bus_area, disct_time_series, on="area_id", how="inner"
            )

        elif carrier == "rural_heat":
            # interlinking heat_bus_id and mv_grid bus_id
            bus_sub = db.select_dataframe(
                f"""
                 SELECT {targets['heat_buses']['schema']}.
                 {targets['heat_buses']['table']}.bus_id as heat_bus_id, 
                 {sources['egon_mv_grid_district']['schema']}.
                             {sources['egon_mv_grid_district']['table']}.bus_id as 
                             bus_id FROM
                 {targets['heat_buses']['schema']}.
                 {targets['heat_buses']['table']}
                 JOIN {sources['egon_mv_grid_district']['schema']}.
                             {sources['egon_mv_grid_district']['table']}
                 ON ST_Transform(ST_Centroid({sources['egon_mv_grid_district']['schema']}.
                             {sources['egon_mv_grid_district']['table']}.geom), 
                                 4326) = {targets['heat_buses']['schema']}.
                                         {targets['heat_buses']['table']}.geom
                 WHERE carrier = '{carrier}'
                 AND scn_name = '{scenario}'
                 """
            )
            ##**scenario name still needs to be adjusted in bus_sub**

            # individual heating time series
            ind_time_series = db.select_dataframe(
                f"""
                SELECT scenario, bus_id, dist_aggregated_mw FROM 
                demand.egon_etrago_timeseries_individual_heating
                WHERE scenario ='{scenario}'
                AND carrier = 'heat_pump'
                """
            )

            # bus_id connected to corresponding time series
            bus_ts = pd.merge(
                bus_sub, ind_time_series, on="bus_id", how="inner"
            )

            # Connect  heat loads to heat buses
            bus_ts.loc[:, "bus_id"] = bus_ts.loc[:, "heat_bus_id"]

        else:
            efficiency_gas_boiler = get_sector_parameters("heat", "eGon2035")[
                "efficiency"
            ]["rural_gas_boiler"]

            # Select rural heat demand coverd by individual gas boilers
            ind_time_series = db.select_dataframe(
                f"""
                SELECT * FROM 
                demand.egon_etrago_timeseries_individual_heating
                WHERE scenario ='{scenario}'
                AND carrier = 'CH4'
                """
            )

            # Select geoetry of medium voltage grid districts
            mvgd_geom = db.select_geodataframe(
                f"""
                SELECT bus_id, ST_CENTROID(geom) as geom FROM 
                {sources['egon_mv_grid_district']['schema']}.
                {sources['egon_mv_grid_district']['table']}
                """
            )

            # Select geometry of gas (CH4) voronoi
            gas_voronoi = db.select_geodataframe(
                f"""
                SELECT bus_id, geom FROM 
                grid.egon_gas_voronoi
                WHERE scn_name = '{scenario}'
                AND carrier = 'CH4'
                """
            )

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
        db.execute_sql(
            f"""
            DELETE FROM grid.egon_etrago_load
            WHERE scn_name = '{scenario}'
            AND carrier = '{carrier}'
            """
        )

        db.execute_sql(
            f"""
            DELETE FROM
            grid.egon_etrago_load_timeseries
            WHERE scn_name = '{scenario}'
            AND load_id NOT IN (
            SELECT load_id FROM
            grid.egon_etrago_load
            WHERE scn_name = '{scenario}')
            """
        )

        next_id = next_etrago_id("load")

        bus_ts["load_id"] = np.arange(len(bus_ts)) + next_id

        etrago_load = pd.DataFrame(index=range(len(bus_ts)))
        etrago_load["scn_name"] = scenario
        etrago_load["load_id"] = bus_ts.load_id
        etrago_load["bus"] = bus_ts.bus_id
        etrago_load["carrier"] = carrier
        etrago_load["sign"] = -1

        etrago_load.to_sql(
            "egon_etrago_load",
            schema="grid",
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
            "egon_etrago_load_timeseries",
            schema="grid",
            con=db.engine(),
            if_exists="append",
            index=False,
        )


class HtsEtragoTable(Dataset):
    """
    Collect heat demand time series for the eTraGo tool

    This dataset collects data for individual and district heating demands
    and writes that into the tables that can be read by the eTraGo tool.

    *Dependencies*
      * :py:class:`DistrictHeatingAreas <egon.data.datasets.district_heating_areas.DistrictHeatingAreas>`
      * :py:class:`HeatEtrago <egon.data.datasets.heat_etrago.HeatEtrago>`
      * :py:func:`define_mv_grid_districts <egon.data.datasets.mv_grid_districts.define_mv_grid_districts>`
      * :py:class:`HeatPumps2035 <egon.data.datasets.heat_supply.individual_heating.HeatPumps2035>`
      * :py:class:`HeatTimeSeries <egon.data.datasets.heat_demand_timeseries.HeatTimeSeries>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_load <egon.data.datasets.etrago_setup.EgonPfHvLoad>` is extended
      * :py:class:`grid.egon_etrago_load_timeseries <egon.data.datasets.etrago_setup.EgonPfHvLoadTimeseries>` is extended

    """

    #:
    name: str = "HtsEtragoTable"
    #:
    version: str = "0.0.6"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(hts_to_etrago,),
        )
