"""The central module containing code to merge data on electricity demand
and feed this data into the corresponding etraGo tables.

"""
from datetime import datetime
from pathlib import Path

import os
import egon.data.config
import pandas as pd
from egon.data import db
from egon.data.datasets import Dataset


def demands_per_bus(scenario):
    """Sum all electricity demand curves up per bus

    Parameters
    ----------
    scenario : str
        Scenario name.

    Returns
    -------
    pandas.DataFrame
        Aggregated electrical demand timeseries per bus
    """

    # Read information from configuration file
    sources = egon.data.config.datasets()["etrago_electricity"]["sources"]

    # Select data on CTS electricity demands per bus
    cts_curves = db.select_dataframe(
        f"""SELECT bus_id AS bus, p_set FROM
                {sources['cts_curves']['schema']}.
                {sources['cts_curves']['table']}
                WHERE scn_name = '{scenario}'""",
    )

    # Select data on industrial demands assigned to osm landuse areas

    ind_curves_osm = db.select_dataframe(
        f"""SELECT bus, p_set FROM
                {sources['osm_curves']['schema']}.
                {sources['osm_curves']['table']}
                WHERE scn_name = '{scenario}'""",
    )

    # Select data on industrial demands assigned to industrial sites

    ind_curves_sites = db.select_dataframe(
        f"""SELECT bus, p_set FROM
                {sources['sites_curves']['schema']}.
                {sources['sites_curves']['table']}
                WHERE scn_name = '{scenario}'""",
    )

    # Select data on household electricity demands per bus

    hh_curves = db.select_dataframe(
        f"""SELECT bus_id AS bus, p_set FROM
                {sources['household_curves']['schema']}.
                {sources['household_curves']['table']}
                WHERE scn_name = '{scenario}'""",
    )

    # Create one df by appending all imported dataframes

    demand_curves = pd.concat(
        [cts_curves, ind_curves_osm, ind_curves_sites, hh_curves],
        ignore_index=True,
    ).set_index("bus")

    # Split array to single columns in the dataframe
    demand_curves_split = demand_curves

    demand_curves_split = pd.DataFrame(
        demand_curves.p_set.tolist(), index=demand_curves_split.index
    )

    # Group all rows with the same bus
    demand_curves_bus = demand_curves_split.groupby(
        demand_curves_split.index
    ).sum()

    # Initzialize and fill resulsting dataframe
    curves = pd.DataFrame(columns=["bus", "p_set"])
    curves["bus"] = demand_curves_bus.index
    curves["p_set"] = demand_curves_bus.values.tolist()

    # Store national demand time series for pypsa-eur-sec
    store_national_profiles(
        ind_curves_sites,
        ind_curves_osm,
        cts_curves,
        hh_curves,
        scenario,
    )

    return curves


def store_national_profiles(
    ind_curves_sites,
    ind_curves_osm,
    cts_curves,
    hh_curves,
    scenario,
):
    """
    Store electrical load timeseries aggregated for national level as an
    input for pypsa-eur-sec

    Parameters
    ----------
    ind_curves_sites : pd.DataFrame
        Industrial load timeseries for industrial sites per bus
    ind_curves_osm : pd.DataFrame
        Industrial load timeseries for industrial osm areas per bus
    cts_curves : pd.DataFrame
        CTS load curves per bus
    hh_curves : pd.DataFrame
        Household load curves per bus
    scenario : str
        Scenario name

    Returns
    -------
    None.

    """

    folder = Path(".") / "input-pypsa-eur-sec"
    # Create the folder, if it does not exists already
    if not os.path.exists(folder):
        os.mkdir(folder)

    national_demand = pd.DataFrame(
        columns=["residential_and_service", "industry"],
        index=pd.date_range(datetime(2011, 1, 1, 0), periods=8760, freq="H"),
    )

    national_demand["industry"] = (
        pd.DataFrame(ind_curves_sites.p_set.tolist()).sum()
        + pd.DataFrame(ind_curves_osm.p_set.tolist()).sum()
    ).values

    national_demand["residential_and_service"] = (
        pd.DataFrame(cts_curves.p_set.tolist()).sum()
        + pd.DataFrame(hh_curves.p_set.tolist()).sum()
    ).values

    national_demand.to_csv(
        folder / f"electrical_demand_timeseries_DE_{scenario}.csv"
    )


def export_to_db():
    """Prepare and export eTraGo-ready information of loads per bus and their
    time series to the database

    Returns
    -------
    None.

    """
    sources = egon.data.config.datasets()["etrago_electricity"]["sources"]
    targets = egon.data.config.datasets()["etrago_electricity"]["targets"]

    for scenario in egon.data.config.settings()["egon-data"]["--scenarios"]:
        # Delete existing data from database
        db.execute_sql(
            f"""
            DELETE FROM
            {targets['etrago_load']['schema']}.{targets['etrago_load']['table']}
            WHERE scn_name = '{scenario}'
            AND carrier = 'AC'
            AND bus IN (
                SELECT bus_id FROM
                {sources['etrago_buses']['schema']}.
                {sources['etrago_buses']['table']}
                WHERE country = 'DE'
                AND carrier = 'AC'
                AND scn_name = '{scenario}')
            """
        )

        db.execute_sql(
            f"""
            DELETE FROM
            {targets['etrago_load_curves']['schema']}.{targets['etrago_load_curves']['table']}
            WHERE scn_name = '{scenario}'
            AND load_id NOT IN (
            SELECT load_id FROM
            {targets['etrago_load']['schema']}.
            {targets['etrago_load']['table']}
            WHERE scn_name = '{scenario}')
            """
        )

        curves = demands_per_bus(scenario)

        # Initialize dataframes equivalent to database tables

        load = pd.DataFrame(
            columns=[
                "scn_name",
                "load_id",
                "bus",
                "type",
                "carrier",
                "p_set",
                "q_set",
                "sign",
            ]
        )
        load_timeseries = pd.DataFrame(
            columns=["scn_name", "load_id", "temp_id", "p_set", "q_set"]
        )

        # Choose next unused load_id
        next_load_id = db.next_etrago_id("load")

        # Insert values into load df
        load.bus = curves.bus
        load.scn_name = scenario
        load.sign = -1
        load.carrier = "AC"
        load.load_id = range(next_load_id, next_load_id + len(load))
        load.p_set = curves.p_set

        # Insert values into load timeseries df
        load_timeseries[["load_id", "p_set"]] = load[["load_id", "p_set"]]
        load_timeseries.scn_name = scenario
        load_timeseries.temp_id = 1

        # Delete p_set column from load df
        load.drop(columns=["p_set"], inplace=True)

        # Set index

        load = load.set_index(["scn_name", "load_id"])
        load_timeseries = load_timeseries.set_index(
            ["scn_name", "load_id", "temp_id"]
        )

        # Insert data into database
        load.to_sql(
            targets["etrago_load"]["table"],
            schema=targets["etrago_load"]["schema"],
            con=db.engine(),
            if_exists="append",
        )

        load_timeseries.to_sql(
            targets["etrago_load_curves"]["table"],
            schema=targets["etrago_load_curves"]["schema"],
            con=db.engine(),
            if_exists="append",
        )


class ElectricalLoadEtrago(Dataset):
    """
    Aggregate annual and hourly electricity demands per substation and export
    to eTraGo tables

    All loads including time series are aggregated per corresponding substation
    and inserted into the existing eTraGo tables. Additionally the aggregated
    national time series are stored to function as an input for pypsa-eur-sec.


    *Dependencies*
      * :py:class:`CtsElectricityDemand <egon.data.datasets.electricity_demand.CtsElectricityDemand>`
      * :py:class:`IndustrialDemandCurves <egon.data.datasets.industry.IndustrialDemandCurves>`
      * :py:class:`hh_buildings <egon.data.datasets.electricity_demand_timeseries.hh_buildings>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_load <egon.data.datasets.etrago_setup.EgonPfHvLoad>` is extended
      * :py:class:`grid.egon_etrago_load_timeseries <egon.data.datasets.etrago_setup.EgonPfHvLoadTimeseries>` is extended
    """

    #:
    name: str = "Electrical_load_etrago"
    #:
    version: str = "0.0.8"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(export_to_db,),
        )
