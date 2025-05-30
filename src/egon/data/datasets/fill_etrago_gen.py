import geopandas as gpd
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.datasets.scenario_parameters import get_sector_parameters
import egon.data.config


class Egon_etrago_gen(Dataset):
    """
    Group generators based on Scenario, carrier and bus. Marginal costs are
    assigned to generators without this data. Grouped generators
    are sent to the egon_etrago_generator table and a timeseries is assigned
    to the weather dependent ones.

    *Dependencies*
      * :py:class:`PowerPlants <egon.data.datasets.power_plants.PowerPlants>`
      * :py:class:`WeatherData <egon.data.datasets.era5.WeatherData>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_generator
        <egon.data.datasets.etrago_setup.EgonPfHvGenerator>` is extended
      * :py:class:`grid.egon_etrago_generator_timeseries
        <egon.data.datasets.etrago_setup.EgonPfHvGeneratorTimeseries>` is filled

    """
    #:
    name: str = "etrago_generators"
    #:
    version: str = "0.0.8"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
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

    # Delete power plants from previous iterations of this script
    delete_previuos_gen(cfg, con, etrago_gen_orig, power_plants)

    renew_feedin = adjust_renew_feedin_table(
        renew_feedin=renew_feedin, cfg=cfg
    )

    etrago_pp = group_power_plants(
        power_plants=power_plants,
        renew_feedin=renew_feedin,
        etrago_gen_orig=etrago_gen_orig,
        cfg=cfg,
    )

    etrago_pp = add_marginal_costs(etrago_pp)

    etrago_gen_table = fill_etrago_gen_table(
        etrago_pp2=etrago_pp, etrago_gen_orig=etrago_gen_orig, cfg=cfg, con=con
    )

    etrago_gen_time_table = fill_etrago_gen_time_table(
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
        "carrier": consistency,
        "el_capacity": np.sum,
        "bus_id": consistency,
        "weather_cell_id": power_timeser,
        "scenario": consistency,
    }

    etrago_pp = power_plants.groupby(by=["bus_id", "carrier", "scenario"]).agg(
        func=agg_func
    )
    etrago_pp = etrago_pp.reset_index(drop=True)

    max_id = db.next_etrago_id("generator")
    etrago_pp["generator_id"] = list(range(max_id, max_id + len(etrago_pp)))
    etrago_pp.set_index("generator_id", inplace=True)

    return etrago_pp


def add_marginal_costs(power_plants):
    scenarios = power_plants.scenario.unique()
    pp = pd.DataFrame()

    for scenario in scenarios:
        pp_scn = power_plants[power_plants["scenario"] == scenario].copy()
        # Read marginal costs from scenario capacities
        marginal_costs = pd.DataFrame.from_dict(
            get_sector_parameters("electricity", scenario)["marginal_cost"],
            orient="index",
        ).rename(columns={0: "marginal_cost"})

        # Set marginal costs = 0 for technologies without values
        warning = []
        for carrier in pp_scn.carrier.unique():
            if carrier not in (marginal_costs.index):
                warning.append(carrier)
                marginal_costs.at[carrier, "marginal_cost"] = 0
        if warning:
            print(
                f"""There are no marginal_cost values for: \n{warning}
        in the scenario {scenario}. Missing values set to 0"""
            )
        pp = pd.concat(
            [
                pp,
                pp_scn.merge(
                    right=marginal_costs, left_on="carrier", right_index=True
                ),
            ]
        )

    return pp


def fill_etrago_gen_table(etrago_pp2, etrago_gen_orig, cfg, con):
    etrago_pp = etrago_pp2[
        ["carrier", "el_capacity", "bus_id", "scenario", "marginal_cost"]
    ]
    etrago_pp = etrago_pp.rename(
        columns={
            "el_capacity": "p_nom",
            "bus_id": "bus",
            "scenario": "scn_name",
        }
    )

    etrago_pp.to_sql(
        name=f"{cfg['targets']['etrago_generators']['table']}",
        schema=f"{cfg['targets']['etrago_generators']['schema']}",
        con=con,
        if_exists="append",
    )
    return etrago_pp


def fill_etrago_gen_time_table(
    etrago_pp, power_plants, renew_feedin, pp_time, cfg, con
):
    etrago_pp_time = etrago_pp.copy()
    etrago_pp_time = etrago_pp_time[
        ["carrier", "el_capacity", "bus_id", "weather_cell_id", "scenario"]
    ]

    etrago_pp_time = etrago_pp_time[
        (etrago_pp_time["carrier"] == "solar")
        | (etrago_pp_time["carrier"] == "solar_rooftop")
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

    etrago_pp_time.to_sql(
        name=f"{cfg['targets']['etrago_gen_time']['table']}",
        schema=f"{cfg['targets']['etrago_gen_time']['schema']}",
        con=con,
        if_exists="append",
    )
    return etrago_pp_time


def load_tables(con, cfg):
    sql = f"""
    SELECT * FROM
    {cfg['sources']['power_plants']['schema']}.
    {cfg['sources']['power_plants']['table']}
    WHERE carrier != 'gas'
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
    if weather_data.isna().any():
        return -1
    else:
        return weather_data.iloc[0]


def adjust_renew_feedin_table(renew_feedin, cfg):
    # Define carrier 'pv' as 'solar'
    carrier_pv_mask = renew_feedin["carrier"] == "pv"
    renew_feedin.loc[carrier_pv_mask, "carrier"] = "solar"

    # Copy solar timeseries for solar_rooftop
    feedin_solar_rooftop = renew_feedin.loc[renew_feedin["carrier"]=="solar"]
    feedin_solar_rooftop.loc[:, "carrier"] = "solar_rooftop"
    renew_feedin = pd.concat([renew_feedin, feedin_solar_rooftop], ignore_index=True)

    # convert renewable feedin lists to arrays
    renew_feedin["feedin"] = renew_feedin["feedin"].apply(np.array)

    return renew_feedin


def delete_previuos_gen(cfg, con, etrago_gen_orig, power_plants):
    for scn_name in egon.data.config.settings()["egon-data"]["--scenarios"]:
        power_plants_scn = power_plants[power_plants["scenario"] == scn_name]
        carrier_delete = list(power_plants_scn.carrier.unique())

        if carrier_delete:
            db.execute_sql(
                f"""DELETE FROM
                        {cfg['targets']['etrago_generators']['schema']}.
                        {cfg['targets']['etrago_generators']['table']}
                        WHERE carrier IN {*carrier_delete,}
                        AND bus IN (
                            SELECT bus_id FROM {cfg['sources']['bus']['schema']}.
                            {cfg['sources']['bus']['table']}
                            WHERE country = 'DE'
                            AND carrier = 'AC'
                            AND scn_name = '{scn_name}')
                        AND scn_name ='{scn_name}'
                        """
            )

            db.execute_sql(
                f"""DELETE FROM
                        {cfg['targets']['etrago_gen_time']['schema']}.
                        {cfg['targets']['etrago_gen_time']['table']}
                        WHERE generator_id NOT IN (
                            SELECT generator_id FROM
                            {cfg['targets']['etrago_generators']['schema']}.
                            {cfg['targets']['etrago_generators']['table']})
                        AND scn_name ='{scn_name}'
                        """
            )


def set_timeseries(power_plants, renew_feedin):
    """
    Create a function to calculate the feed-in timeseries for power plants.

    Parameters
    ----------
    power_plants : DataFrame
        A DataFrame containing information about power plants, including their bus IDs,
        carriers, weather cell IDs, and electrical capacities.
    renew_feedin : DataFrame
        A DataFrame containing feed-in values for different carriers and weather cell IDs.

    Returns
    -------
    function
        A function that takes a power plant object and returns its feed-in value based on
        either its direct weather cell ID or the aggregated feed-in of all power plants
        connected to the same bus and having the same carrier.
    """

    def timeseries(pp):
        """Calculate the feed-in for a given power plant based on weather cell ID or aggregation."""
        if pp.weather_cell_id != -1:
            # Directly fetch feed-in value for power plants with an associated weather cell ID
            return renew_feedin.loc[
                (renew_feedin["w_id"] == pp.weather_cell_id)
                & (renew_feedin["carrier"] == pp.carrier),
                "feedin",
            ].iat[0]
        else:
            # Aggregate feed-in for power plants without a direct weather cell association
            df = power_plants.loc[
                (power_plants["bus_id"] == pp.bus_id)
                & (power_plants["carrier"] == pp.carrier)
            ].dropna(subset=["weather_cell_id"])

            total_int_cap = df["el_capacity"].sum()

            # Fetch and calculate proportional feed-in for each power plant
            df["feedin"] = df.apply(
                lambda x: renew_feedin.loc[
                    (renew_feedin["w_id"] == x.weather_cell_id)
                    & (renew_feedin["carrier"] == x.carrier),
                    "feedin",
                ].iat[0],
                axis=1,
            )

            # Calculate and return aggregated feed-in based on electrical capacity
            return df.apply(
                lambda x: x["el_capacity"] / total_int_cap * x["feedin"], axis=1
            ).sum()

    return timeseries
