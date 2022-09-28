from egon.data import config, db
from egon.data.db import next_etrago_id
from egon.data.datasets import Dataset
from egon.data.datasets.heat_demand_timeseries import (
    individual_heating_per_mv_grid_tables,
    individual_heating_per_mv_grid_2035,
    individual_heating_per_mv_grid_100,
)

import pandas as pd
import numpy as np


def hts_to_etrago():

    sources = config.datasets()["etrago_heat"]["sources"]
    targets = config.datasets()["etrago_heat"]["targets"]
    scenario = "eGon2035"
    carriers = ["central_heat", "rural_heat"]
    # Temporary drop everything related to rural heat
    carriers = ["central_heat"]

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

        else:
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
                                SELECT * FROM 
                                demand.egon_etrago_timeseries_individual_heating
                                WHERE scenario ='{scenario}'
                                """
            )

            # bus_id connected to corresponding time series
            bus_ts = pd.merge(
                bus_sub, ind_time_series, on="bus_id", how="inner"
            )

            # Connect  heat loads to heat buses
            bus_ts.loc[:, "bus_id"] = bus_ts.loc[:, "heat_bus_id"]

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
        etrago_load_timeseries["p_set"] = bus_ts.iloc[:, 3]

        etrago_load_timeseries.to_sql(
            "egon_etrago_load_timeseries",
            schema="grid",
            con=db.engine(),
            if_exists="append",
            index=False,
        )


class HtsEtragoTable(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HtsEtragoTable",
            version="0.0.5",
            dependencies=dependencies,
            tasks=(
                # Temporary drop everything related to rural heat
                # individual_heating_per_mv_grid_tables,
                # {
                #     individual_heating_per_mv_grid_2035,
                #     individual_heating_per_mv_grid_100,
                # },
                hts_to_etrago,
            ),
        )
