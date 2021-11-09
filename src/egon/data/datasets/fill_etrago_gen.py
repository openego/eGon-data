import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset
import egon.data.config

class Egon_etrago_gen(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="etrago_generators",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(fill_etrago_generators,),
        )


def fill_etrago_generators():
    # Connect to the data base
    con = db.engine()
    cfg = egon.data.config.datasets()["generators_etrago"]

    # Load required tables
    (
        power_plants,
        renew_feedin,
        weather_cells,
        etrago_gen_orig,
        pp_time,
    ) = load_tables(con, cfg)

    power_plants = adjust_power_plants_table(
        power_plants=power_plants, renew_feedin=renew_feedin, cfg=cfg
    )

    delete_previuos_gen(cfg)

    etrago_pp = group_power_plants(
        power_plants=power_plants,
        renew_feedin=renew_feedin,
        etrago_gen_orig=etrago_gen_orig,
        cfg=cfg,
    )

    fill_etrago_gen_table(
        etrago_pp2=etrago_pp, etrago_gen_orig=etrago_gen_orig, cfg=cfg, con=con
    )

    fill_etrago_gen_time_table(
        etrago_pp=etrago_pp,
        power_plants=power_plants,
        renew_feedin=renew_feedin,
        pp_time=pp_time,
        cfg=cfg,
        con=con,
    )

    return "eTrago Generators tables filled successfully"


def group_power_plants(power_plants, renew_feedin, etrago_gen_orig, cfg):

    # group power plants by bus and carrier

    agg_func = {
        "sources": numpy_nan,
        "source_id": numpy_nan,
        "carrier": consistency,
        "chp": numpy_nan,  # consistency,
        "el_capacity": np.sum,
        "th_capacity": numpy_nan,
        "bus_id": consistency,
        "voltage_level": numpy_nan,
        "weather_cell_id": power_timeser,
        "scenario": consistency,
        "geom": numpy_nan,
    }

    etrago_pp = power_plants.groupby(by=["bus_id", "carrier"]).agg(
        func=agg_func
    )
    etrago_pp = etrago_pp.reset_index(drop=True)
    if np.isnan(etrago_gen_orig["generator_id"].max()):
        max_id = 0
    else:
        max_id = etrago_gen_orig["generator_id"].max() + 1
    etrago_pp["generator_id"] = list(range(max_id, max_id + len(etrago_pp)))
    etrago_pp.set_index("generator_id", inplace=True)

    return etrago_pp


def fill_etrago_gen_table(etrago_pp2, etrago_gen_orig, cfg, con):

    etrago_pp = etrago_pp2[["carrier", "el_capacity", "bus_id", "scenario"]]
    etrago_pp = etrago_pp.rename(
        columns={
            "el_capacity": "p_nom",
            "bus_id": "bus",
            "scenario": "scn_name",
        }
    )

    etrago_pp = etrago_pp.reindex(columns=etrago_gen_orig.columns)
    etrago_pp = etrago_pp.drop(columns="generator_id")
    etrago_pp.to_sql(
        name=f"{cfg['targets']['etrago_generators']['table']}",
        schema=f"{cfg['targets']['etrago_generators']['schema']}",
        con=con,
        if_exists="append",
    )
    return "etrago_gen_table filled successfully"


def fill_etrago_gen_time_table(
    etrago_pp, power_plants, renew_feedin, pp_time, cfg, con
):
    etrago_pp_time = etrago_pp.copy()
    etrago_pp_time = etrago_pp_time[
        ["carrier", "el_capacity", "bus_id", "weather_cell_id", "scenario"]
    ]

    etrago_pp_time = etrago_pp_time[
        (etrago_pp_time["carrier"] == "pv")
        | (etrago_pp_time["carrier"] == "wind_onshore")
        | (etrago_pp_time["carrier"] == "wind_offshore")
    ]

    cal_timeseries = set_timeseries(
        power_plants=power_plants, renew_feedin=renew_feedin
    )

    etrago_pp_time["p_max_pu"] = 0
    etrago_pp_time["p_max_pu"] = etrago_pp_time.apply(cal_timeseries, axis=1)

    etrago_pp_time.rename(columns={"scenario": "scn_name"}, inplace=True)
    etrago_pp_time = etrago_pp_time[["scn_name", "p_max_pu"]]
    etrago_pp_time = etrago_pp_time.reindex(columns=pp_time.columns)
    etrago_pp_time = etrago_pp_time.drop(columns="generator_id")
    etrago_pp_time["p_max_pu"] = etrago_pp_time["p_max_pu"].apply(list)
    etrago_pp_time["temp_id"] = 1

    db.execute_sql(
        f"""DELETE FROM 
                   {cfg['targets']['etrago_gen_time']['schema']}.
                   {cfg['targets']['etrago_gen_time']['table']}
                   """
    )

    etrago_pp_time.to_sql(
        name=f"{cfg['targets']['etrago_gen_time']['table']}",
        schema=f"{cfg['targets']['etrago_gen_time']['schema']}",
        con=con,
        if_exists="append",
    )
    return "etrago_gen_time_table was filled successfully"


def load_tables(con, cfg):
    sql = f"""
    SELECT * FROM
    {cfg['sources']['power_plants']['schema']}.
    {cfg['sources']['power_plants']['table']}
    """
    power_plants = gpd.GeoDataFrame.from_postgis(
        sql, con, crs="EPSG:4326", index_col="id"
    )

    sql = f"""
    SELECT * FROM
    {cfg['sources']['renewable_feedin']['schema']}.
    {cfg['sources']['renewable_feedin']['table']}
    """
    renew_feedin = pd.read_sql(sql, con)

    sql = f"""
    SELECT * FROM
    {cfg['sources']['weather_cells']['schema']}.
    {cfg['sources']['weather_cells']['table']}
    """
    weather_cells = gpd.GeoDataFrame.from_postgis(sql, con, crs="EPSG:4326")

    sql = f"""
    SELECT * FROM
    {cfg['targets']['etrago_generators']['schema']}.
    {cfg['targets']['etrago_generators']['table']}
    """
    etrago_gen_orig = pd.read_sql(sql, con)

    sql = f"""
    SELECT * FROM
    {cfg['targets']['etrago_gen_time']['schema']}.
    {cfg['targets']['etrago_gen_time']['table']}
    """
    pp_time = pd.read_sql(sql, con)

    return power_plants, renew_feedin, weather_cells, etrago_gen_orig, pp_time


def consistency(data):
    assert (
        len(set(data)) <= 1
    ), f"The elements in column {data.name} do not match"
    return data.iloc[0]


def numpy_nan(data):
    return np.nan


def power_timeser(weather_data):
    if len(set(weather_data)) <= 1:
        return weather_data.iloc[0]
    else:
        return -1


def adjust_power_plants_table(power_plants, renew_feedin, cfg):
    ################ TASK: DEFINE WHAT TO DO WITH CHP POWER PLANTS ############
    power_plants = power_plants[
        (power_plants["th_capacity"].isnull())
        | (power_plants["th_capacity"] == 0)
    ]

    ################ TASK: DEFINE WHAT TO DO WITH CHP POWER PLANTS ############

    # Define carrier 'solar' as 'pv'
    carrier_pv_mask = power_plants["carrier"] == "solar"
    power_plants.loc[carrier_pv_mask, "carrier"] = "pv"

    # convert renewable feedin lists to arrays
    renew_feedin["feedin"] = renew_feedin["feedin"].apply(np.array)

    return power_plants


def delete_previuos_gen(cfg):
    db.execute_sql(
        f"""DELETE FROM 
                   {cfg['targets']['etrago_generators']['schema']}.
                   {cfg['targets']['etrago_generators']['table']}
                   WHERE carrier <> 'CH4' AND carrier <> 'solar_rooftop'
                   """
    )


def set_timeseries(power_plants, renew_feedin):
    def timeseries(pp):
        try:
            if pp.weather_cell_id != -1:
                feedin_time = renew_feedin[
                    (renew_feedin["w_id"] == pp.weather_cell_id)
                    & (renew_feedin["carrier"] == pp.carrier)
                ].feedin.iloc[0]
                return feedin_time
            else:
                df = power_plants[
                    (power_plants["bus_id"] == pp.bus_id)
                    & (power_plants["carrier"] == pp.carrier)
                ]
                total_int_cap = df.el_capacity.sum()
                df["feedin"] = 0
                df["feedin"] = df.apply(
                    lambda x: renew_feedin[
                        (renew_feedin["w_id"] == x.weather_cell_id)
                        & (renew_feedin["carrier"] == x.carrier)
                    ].feedin.iloc[0],
                    axis=1,
                )
                df["feedin"] = df.apply(
                    lambda x: x.el_capacity / total_int_cap * x.feedin, axis=1
                )
                return df.feedin.sum()
        #######################################################################
        ####################### DELETE THIS EXCEPTION #########################
        except:
            df = power_plants[
                (power_plants["bus_id"] == pp.bus_id)
                & (power_plants["carrier"] == pp.carrier)
            ]
            return list(df.weather_cell_id)

    ####################### DELETE THIS EXCEPTION #############################
    ###########################################################################
    return timeseries
