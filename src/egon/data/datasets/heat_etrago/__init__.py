"""The central module containing all code dealing with heat sector in etrago
"""
import pandas as pd
import geopandas as gpd
from egon.data import db, config
from egon.data.datasets.heat_etrago.power_to_heat import (
    insert_central_power_to_heat,
    insert_individual_power_to_heat,
)
from egon.data.datasets import Dataset
from egon.data.datasets.etrago_setup import link_geom_from_buses
from egon.data.datasets.scenario_parameters import get_sector_parameters


def insert_buses(carrier, scenario):
    """Insert heat buses to etrago table

    Heat buses are divided into central and individual heating

    Parameters
    ----------
    carrier : str
        Name of the carrier, either 'central_heat' or 'rural_heat'
    scenario : str, optional
        Name of the scenario.

    """
    sources = config.datasets()["etrago_heat"]["sources"]
    target = config.datasets()["etrago_heat"]["targets"]["heat_buses"]
    # Delete existing heat buses (central or rural)
    db.execute_sql(
        f"""
        DELETE FROM {target['schema']}.{target['table']}
        WHERE scn_name = '{scenario}'
        AND carrier = '{carrier}'
        AND country = 'DE'
        """
    )

    # Select unused index of buses
    next_bus_id = db.next_etrago_id("bus")

    # initalize dataframe for heat buses
    heat_buses = (
        gpd.GeoDataFrame(
            columns=["scn_name", "bus_id", "carrier", "x", "y", "geom"]
        )
        .set_geometry("geom")
        .set_crs(epsg=4326)
    )

    # If central heat, create one bus per district heating area
    if carrier == "central_heat":
        areas = db.select_geodataframe(
            f"""
            SELECT area_id, geom_polygon as geom
            FROM  {sources['district_heating_areas']['schema']}.
            {sources['district_heating_areas']['table']}
            WHERE scenario = '{scenario}'
            """,
            index_col="area_id",
        )
        heat_buses.geom = areas.centroid.to_crs(epsg=4326)
    # otherwise create one heat bus per hvmv substation
    # which represents aggregated individual heating for etrago
    else:
        mv_grids = db.select_geodataframe(
            f"""
            SELECT ST_Centroid(geom) AS geom
            FROM {sources['mv_grids']['schema']}.
            {sources['mv_grids']['table']}
            WHERE bus_id IN
                (SELECT DISTINCT bus_id
                FROM boundaries.egon_map_zensus_grid_districts a
                JOIN demand.egon_peta_heat b
                ON a.zensus_population_id = b.zensus_population_id
                WHERE b.scenario = '{scenario}'
                AND b.zensus_population_id NOT IN (
                SELECT zensus_population_id FROM
                	demand.egon_map_zensus_district_heating_areas
                	WHERE scenario = '{scenario}'
                )
            )
            """
        )
        heat_buses.geom = mv_grids.geom.to_crs(epsg=4326)

    # Insert values into dataframe
    heat_buses.scn_name = scenario
    heat_buses.carrier = carrier
    heat_buses.x = heat_buses.geom.x
    heat_buses.y = heat_buses.geom.y
    heat_buses.bus_id = range(next_bus_id, next_bus_id + len(heat_buses))

    # Insert data into database
    heat_buses.to_postgis(
        target["table"],
        schema=target["schema"],
        if_exists="append",
        con=db.engine(),
    )


def insert_store(scenario, carrier):
    sources = config.datasets()["etrago_heat"]["sources"]
    targets = config.datasets()["etrago_heat"]["targets"]

    db.execute_sql(
        f"""
        DELETE FROM {targets['heat_buses']['schema']}.
        {targets['heat_buses']['table']}
        WHERE carrier = '{carrier}_store'
        AND scn_name = '{scenario}'
        AND country = 'DE'
        """
    )
    db.execute_sql(
        f"""
        DELETE FROM {targets['heat_links']['schema']}.
        {targets['heat_links']['table']}
        WHERE carrier LIKE '{carrier}_store%'
        AND scn_name = '{scenario}'
        AND bus0 IN
        (SELECT bus_id
         FROM {targets['heat_buses']['schema']}.
         {targets['heat_buses']['table']}
         WHERE scn_name = '{scenario}'
         AND country = 'DE')
        AND bus1 IN
        (SELECT bus_id
         FROM {targets['heat_buses']['schema']}.
         {targets['heat_buses']['table']}
         WHERE scn_name = '{scenario}'
         AND country = 'DE')
        """
    )
    db.execute_sql(
        f"""
        DELETE FROM {targets['heat_stores']['schema']}.
        {targets['heat_stores']['table']}
        WHERE carrier = '{carrier}_store'
        AND scn_name = '{scenario}'
        AND bus IN
        (SELECT bus_id
         FROM {targets['heat_buses']['schema']}.
         {targets['heat_buses']['table']}
         WHERE scn_name = '{scenario}'
         AND country = 'DE')
        """
    )

    dh_bus = db.select_geodataframe(
        f"""
        SELECT * FROM
        {targets['heat_buses']['schema']}.
        {targets['heat_buses']['table']}
        WHERE carrier = '{carrier}'
        AND scn_name = '{scenario}'
        AND country = 'DE'
        """,
        epsg=4326,
    )

    water_tank_bus = dh_bus.copy()
    water_tank_bus.carrier = carrier + "_store"
    water_tank_bus.bus_id = range(
        db.next_etrago_id("bus"),
        db.next_etrago_id("bus") + len(water_tank_bus),
    )

    water_tank_bus.to_postgis(
        targets["heat_buses"]["table"],
        schema=targets["heat_buses"]["schema"],
        con=db.engine(),
        if_exists="append",
        index=False,
    )

    water_tank_charger = pd.DataFrame(
        data={
            "scn_name": scenario,
            "bus0": dh_bus.bus_id,
            "bus1": water_tank_bus.bus_id,
            "carrier": carrier + "_store_charger",
            "efficiency": get_sector_parameters("heat", "eGon2035")[
                "efficiency"
            ]["water_tank_charger"],
            "marginal_cost": get_sector_parameters("heat", "eGon2035")[
                "marginal_cost"
            ]["water_tank_charger"],
            "p_nom_extendable": True,
            "link_id": range(
                db.next_etrago_id("link"),
                db.next_etrago_id("link") + len(water_tank_bus),
            ),
        }
    )

    water_tank_charger.to_sql(
        targets["heat_links"]["table"],
        schema=targets["heat_links"]["schema"],
        con=db.engine(),
        if_exists="append",
        index=False,
    )

    water_tank_discharger = pd.DataFrame(
        data={
            "scn_name": scenario,
            "bus0": water_tank_bus.bus_id,
            "bus1": dh_bus.bus_id,
            "carrier": carrier + "_store_discharger",
            "efficiency": get_sector_parameters("heat", "eGon2035")[
                "efficiency"
            ]["water_tank_discharger"],
            "marginal_cost": get_sector_parameters("heat", "eGon2035")[
                "marginal_cost"
            ]["water_tank_discharger"],
            "p_nom_extendable": True,
            "link_id": range(
                db.next_etrago_id("link"),
                db.next_etrago_id("link") + len(water_tank_bus),
            ),
        }
    )

    water_tank_discharger.to_sql(
        targets["heat_links"]["table"],
        schema=targets["heat_links"]["schema"],
        con=db.engine(),
        if_exists="append",
        index=False,
    )

    water_tank_store = pd.DataFrame(
        data={
            "scn_name": scenario,
            "bus": water_tank_bus.bus_id,
            "carrier": carrier + "_store",
            "capital_cost": get_sector_parameters("heat", "eGon2035")[
                "capital_cost"
            ][f"{carrier.split('_')[0]}_water_tank"],
            "lifetime": get_sector_parameters("heat", "eGon2035")["lifetime"][
                f"{carrier.split('_')[0]}_water_tank"
            ],
            "e_nom_extendable": True,
            "store_id": range(
                db.next_etrago_id("store"),
                db.next_etrago_id("store") + len(water_tank_bus),
            ),
        }
    )

    water_tank_store.to_sql(
        targets["heat_stores"]["table"],
        schema=targets["heat_stores"]["schema"],
        con=db.engine(),
        if_exists="append",
        index=False,
    )


def store():
    insert_store("eGon2035", "central_heat")
    insert_store("eGon2035", "rural_heat")


def insert_central_direct_heat(scenario="eGon2035"):
    """Insert renewable heating technologies (solar and geo thermal)

    Parameters
    ----------
    scenario : str, optional
        Name of the scenario The default is 'eGon2035'.

    Returns
    -------
    None.

    """
    sources = config.datasets()["etrago_heat"]["sources"]
    targets = config.datasets()["etrago_heat"]["targets"]

    db.execute_sql(
        f"""
        DELETE FROM {targets['heat_generators']['schema']}.
        {targets['heat_generators']['table']}
        WHERE carrier IN ('solar_thermal_collector', 'geo_thermal')
        AND scn_name = '{scenario}'
        AND bus IN
        (SELECT bus_id
         FROM {targets['heat_buses']['schema']}.
         {targets['heat_buses']['table']}
         WHERE scn_name = '{scenario}'
         AND country = 'DE')
        """
    )

    db.execute_sql(
        f"""
        DELETE FROM {targets['heat_generator_timeseries']['schema']}.
        {targets['heat_generator_timeseries']['table']}
        WHERE scn_name = '{scenario}'
        AND generator_id NOT IN (
            SELECT generator_id FROM
            {targets['heat_generators']['schema']}.
            {targets['heat_generators']['table']}
            WHERE scn_name = '{scenario}')
        """
    )

    central_thermal = db.select_geodataframe(
        f"""
            SELECT district_heating_id, capacity, geometry, carrier
            FROM  {sources['district_heating_supply']['schema']}.
            {sources['district_heating_supply']['table']}
            WHERE scenario = '{scenario}'
            AND carrier IN (
                'solar_thermal_collector', 'geo_thermal')
            """,
        geom_col="geometry",
        index_col="district_heating_id",
    )

    map_dh_id_bus_id = db.select_dataframe(
        f"""
        SELECT bus_id, area_id, id FROM
        {targets['heat_buses']['schema']}.
        {targets['heat_buses']['table']}
        JOIN {sources['district_heating_areas']['schema']}.
            {sources['district_heating_areas']['table']}
        ON ST_Transform(ST_Centroid(geom_polygon), 4326) = geom
        WHERE carrier = 'central_heat'
        AND scenario = '{scenario}'
        AND scn_name = '{scenario}'
        """,
        index_col="id",
    )

    new_id = db.next_etrago_id("generator")

    generator = pd.DataFrame(
        data={
            "scn_name": scenario,
            "carrier": central_thermal.carrier,
            "bus": map_dh_id_bus_id.bus_id[central_thermal.index],
            "p_nom": central_thermal.capacity,
            "generator_id": range(new_id, new_id + len(central_thermal)),
        }
    )

    solar_thermal = central_thermal[
        central_thermal.carrier == "solar_thermal_collector"
    ]

    weather_cells = db.select_geodataframe(
        f"""
        SELECT w_id, geom
        FROM {sources['weather_cells']['schema']}.
            {sources['weather_cells']['table']}
        """,
        index_col="w_id",
    )

    # Map solar thermal collectors to weather cells
    join = gpd.sjoin(weather_cells, solar_thermal)[["index_right"]]

    feedin = db.select_dataframe(
        f"""
        SELECT w_id, feedin
        FROM {sources['feedin_timeseries']['schema']}.
            {sources['feedin_timeseries']['table']}
        WHERE carrier = 'solar_thermal'
        AND weather_year = 2011
        """,
        index_col="w_id",
    )

    timeseries = pd.DataFrame(
        data={
            "scn_name": scenario,
            "temp_id": 1,
            "p_max_pu": feedin.feedin[join.index].values,
            "generator_id": generator.generator_id[
                generator.carrier == "solar_thermal_collector"
            ].values,
        }
    ).set_index("generator_id")

    generator = generator.set_index("generator_id")

    generator.to_sql(
        targets["heat_generators"]["table"],
        schema=targets["heat_generators"]["schema"],
        if_exists="append",
        con=db.engine(),
    )

    timeseries.to_sql(
        targets["heat_generator_timeseries"]["table"],
        schema=targets["heat_generator_timeseries"]["schema"],
        if_exists="append",
        con=db.engine(),
    )


def insert_central_gas_boilers(scenario="eGon2035"):
    """Inserts gas boilers for district heating to eTraGo-table

    Parameters
    ----------
    scenario : str, optional
        Name of the scenario. The default is 'eGon2035'.

    Returns
    -------
    None.

    """

    sources = config.datasets()["etrago_heat"]["sources"]
    targets = config.datasets()["etrago_heat"]["targets"]

    db.execute_sql(
        f"""
        DELETE FROM {targets['heat_links']['schema']}.
        {targets['heat_links']['table']}
        WHERE carrier  LIKE '%central_gas_boiler%'
        AND scn_name = '{scenario}'
        """
    )

    central_boilers = db.select_dataframe(
        f"""
        SELECT c.bus_id as bus0, b.bus_id as bus1,
        capacity, a.carrier, scenario as scn_name
        FROM {sources['district_heating_supply']['schema']}.
        {sources['district_heating_supply']['table']} a
        JOIN {targets['heat_buses']['schema']}.
        {targets['heat_buses']['table']} b
        ON ST_Transform(ST_Centroid(geometry), 4326) = geom
        JOIN {sources['ch4_voronoi']['schema']}.
        {sources['ch4_voronoi']['table']} c
        ON ST_Intersects(ST_Transform(a.geometry, 4326), c.geom)
        WHERE scenario = '{scenario}'
        AND b.scn_name = '{scenario}'
        AND a.carrier = 'gas_boiler'
        AND b.carrier='central_heat'
        AND c.carrier='CH4'
        AND c.scn_name = '{scenario}'
        """
    )

    # Add LineString topology
    central_boilers = link_geom_from_buses(central_boilers, scenario)

    # Add efficiency and marginal costs of gas boilers
    central_boilers["efficiency"] = get_sector_parameters("heat", "eGon2035")[
        "efficiency"
    ]["central_gas_boiler"]
    central_boilers["marginal_cost"] = get_sector_parameters(
        "heat", "eGon2035"
    )["marginal_cost"]["central_gas_boiler"]

    # Transform thermal capacity to CH4 installed capacity
    central_boilers["p_nom"] = central_boilers.capacity.div(
        central_boilers.efficiency
    )

    # Drop unused columns
    central_boilers.drop(["capacity"], axis=1, inplace=True)

    # Set index
    central_boilers.index += db.next_etrago_id("link")
    central_boilers.index.name = "link_id"

    # Set carrier name
    central_boilers.carrier = "central_gas_boiler"

    central_boilers.reset_index().to_postgis(
        targets["heat_links"]["table"],
        schema=targets["heat_links"]["schema"],
        con=db.engine(),
        if_exists="append",
    )


def insert_rural_gas_boilers(scenario="eGon2035"):
    """Inserts gas boilers for individual heating to eTraGo-table

    Parameters
    ----------
    scenario : str, optional
        Name of the scenario. The default is 'eGon2035'.

    Returns
    -------
    None.

    """

    sources = config.datasets()["etrago_heat"]["sources"]
    targets = config.datasets()["etrago_heat"]["targets"]

    db.execute_sql(
        f"""
        DELETE FROM {targets['heat_links']['schema']}.
        {targets['heat_links']['table']}
        WHERE carrier  = 'rural_gas_boiler'
        AND scn_name = '{scenario}'
        AND bus0 IN
        (SELECT bus_id
         FROM {targets['heat_buses']['schema']}.
         {targets['heat_buses']['table']}
         WHERE scn_name = '{scenario}'
         AND country = 'DE')
        AND bus1 IN
        (SELECT bus_id
         FROM {targets['heat_buses']['schema']}.
         {targets['heat_buses']['table']}
         WHERE scn_name = '{scenario}'
         AND country = 'DE')
        """
    )

    rural_boilers = db.select_dataframe(
        f"""
        SELECT c.bus_id as bus0, b.bus_id as bus1,
        capacity, a.carrier, scenario as scn_name
        FROM  {sources['individual_heating_supply']['schema']}.
        {sources['individual_heating_supply']['table']} a
        JOIN {targets['heat_buses']['schema']}.
        {targets['heat_buses']['table']} b
        ON ST_Transform(ST_Centroid(a.geometry), 4326) = b.geom
        JOIN {sources['ch4_voronoi']['schema']}.
        {sources['ch4_voronoi']['table']} c
        ON ST_Intersects(ST_Transform(a.geometry, 4326), c.geom)
        WHERE scenario = '{scenario}'
        AND b.scn_name = '{scenario}'
        AND a.carrier = 'gas_boiler'
        AND b.carrier='rural_heat'
        AND c.carrier='CH4'
        AND c.scn_name = '{scenario}'
        """
    )

    # Add LineString topology
    rural_boilers = link_geom_from_buses(rural_boilers, scenario)

    # Add efficiency of gas boilers
    rural_boilers["efficiency"] = get_sector_parameters("heat", "eGon2035")[
        "efficiency"
    ]["rural_gas_boiler"]

    # Transform thermal capacity to CH4 installed capacity
    rural_boilers["p_nom"] = rural_boilers.capacity.div(
        rural_boilers.efficiency
    )

    # Drop unused columns
    rural_boilers.drop(["capacity"], axis=1, inplace=True)

    # Set index
    rural_boilers.index += db.next_etrago_id("link")
    rural_boilers.index.name = "link_id"

    # Set carrier name
    rural_boilers.carrier = "rural_gas_boiler"

    rural_boilers.reset_index().to_postgis(
        targets["heat_links"]["table"],
        schema=targets["heat_links"]["schema"],
        con=db.engine(),
        if_exists="append",
    )


def buses():
    """Insert individual and district heat buses into eTraGo-tables

    Parameters
    ----------

    Returns
    -------
    None.

    """

    insert_buses("central_heat", scenario="eGon2035")
    insert_buses("rural_heat", scenario="eGon2035")
    insert_buses("central_heat", scenario="eGon100RE")
    insert_buses("rural_heat", scenario="eGon100RE")


def supply():
    """Insert individual and district heat supply into eTraGo-tables

    Parameters
    ----------

    Returns
    -------
    None.

    """

    insert_central_direct_heat(scenario="eGon2035")
    insert_central_power_to_heat(scenario="eGon2035")
    insert_individual_power_to_heat(scenario="eGon2035")

    # insert_rural_gas_boilers(scenario="eGon2035")
    insert_central_gas_boilers(scenario="eGon2035")


class HeatEtrago(Dataset):
    """
    Collect data related to the heat sector for the eTraGo tool

    This dataset collects data from the heat sector and puts it into a format that
    is needed for the transmission grid optimisation within the tool eTraGo.
    It includes the creation of individual and central heat nodes, aggregates the
    heat supply technologies (apart from CHP) per medium voltage grid district and
    adds extendable heat stores to each bus. This data is then writing into the
    corresponding tables that are read by eTraGo.


    *Dependencies*
      * :py:class:`HeatSupply <egon.data.datasets.heat_supply.HeatSupply>`
      * :py:class:`MvGridDistricts <egon.data.datasets.mv_grid_districts.mv_grid_districts_setup>`
      * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:`RenewableFeedin <egon.data.datasets.renewable_feedin.RenewableFeedin>`
      * :py:class:`HeatTimeSeries <egon.data.datasets.heat_demand_timeseries.HeatTimeSeries>`

    *Resulting tables*
      * :py:class:`grid.egon_etrago_bus <egon.data.datasets.etrago_setup.EgonPfHvBus>` is extended
      * :py:class:`grid.egon_etrago_link <egon.data.datasets.etrago_setup.EgonPfHvLink>` is extended
      * :py:class:`grid.egon_etrago_link_timeseries <egon.data.datasets.etrago_setup.EgonPfHvLinkTimeseries>` is extended
      * :py:class:`grid.egon_etrago_store <egon.data.datasets.etrago_setup.EgonPfHvStore>` is extended
      * :py:class:`grid.egon_etrago_generator <egon.data.datasets.etrago_setup.EgonPfHvGenerator>` is extended

    """

    #:
    name: str = "HeatEtrago"
    #:
    version: str = "0.0.10"

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(buses, supply, store),
        )
